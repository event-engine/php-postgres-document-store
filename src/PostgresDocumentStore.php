<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres;

use EventEngine\DocumentStore;
use EventEngine\DocumentStore\Filter\Filter;
use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\OrderBy\OrderBy;
use EventEngine\DocumentStore\PartialSelect;
use EventEngine\DocumentStore\Postgres\Exception\RuntimeException;
use EventEngine\DocumentStore\Postgres\Filter\DefaultFilterProcessor;
use EventEngine\DocumentStore\Postgres\Filter\FilterProcessor;
use EventEngine\Util\VariableType;

use function implode;
use function is_string;
use function json_decode;
use function mb_strlen;
use function mb_substr;
use function sprintf;

final class PostgresDocumentStore implements DocumentStore\DocumentStore
{
    private const PARTIAL_SELECT_DOC_ID = '__partial_sel_doc_id__';
    private const PARTIAL_SELECT_MERGE = '__partial_sel_merge__';

    /**
     * @var \PDO
     */
    private $connection;

    /**
     * @var FilterProcessor
     */
    private $filterProcessor;

    private $tablePrefix = 'em_ds_';

    private $docIdSchema = 'UUID NOT NULL';

    private $manageTransactions;

    private $useMetadataColumns;

    public function __construct(
        \PDO $connection,
        string $tablePrefix = null,
        string $docIdSchema = null,
        bool $transactional = true,
        bool $useMetadataColumns = false,
        FilterProcessor $filterProcessor = null
    ) {
        $this->connection = $connection;
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        if (null === $filterProcessor) {
            $filterProcessor = new DefaultFilterProcessor($useMetadataColumns);
        }
        $this->filterProcessor = $filterProcessor;

        if(null !== $tablePrefix) {
            $this->tablePrefix = $tablePrefix;
        }

        if(null !== $docIdSchema) {
            $this->docIdSchema = $docIdSchema;
        }

        $this->manageTransactions = $transactional;

        $this->useMetadataColumns = $useMetadataColumns;
    }

    /**
     * @return string[] list of all available collections
     */
    public function listCollections(): array
    {
        $prefix = mb_strtolower($this->tablePrefix);
        $query = <<<EOT
SELECT TABLE_NAME 
FROM information_schema.tables
WHERE TABLE_NAME LIKE '{$prefix}%'
EOT;

        $stmt = $this->connection->prepare($query);

        $stmt->execute();

        $collections = [];

        while ($col = $stmt->fetchColumn()) {
            $collections[] = str_replace($prefix, '', $col);
        }

        return $collections;
    }

    /**
     * @param string $prefix
     * @return string[] of collection names
     */
    public function filterCollectionsByPrefix(string $prefix): array
    {
        $tPrefix = mb_strtolower($this->tablePrefix);
        $prefix = mb_strtolower($prefix);
        $query = <<<EOT
SELECT TABLE_NAME 
FROM information_schema.tables
WHERE TABLE_NAME LIKE '{$tPrefix}$prefix%'
EOT;

        $stmt = $this->connection->prepare($query);

        $stmt->execute();

        $collections = [];

        while ($col = $stmt->fetchColumn()) {
            $collections[] = str_replace($tPrefix, '', $col);
        }

        return $collections;
    }

    /**
     * @param string $collectionName
     * @return bool
     */
    public function hasCollection(string $collectionName): bool
    {
        $query = <<<EOT
SELECT TABLE_NAME 
FROM information_schema.tables
WHERE TABLE_NAME = '{$this->tableName($collectionName)}'
AND TABLE_SCHEMA = '{$this->schemaName($collectionName)}'
EOT;

        $stmt = $this->connection->prepare($query);

        $stmt->execute();

        $row = $stmt->fetchColumn();

        return !!$row;
    }

    /**
     * @param string $collectionName
     * @param Index[] ...$indices
     */
    public function addCollection(string $collectionName, Index ...$indices): void
    {
        $metadataColumns = '';

        foreach ($indices as $i => $index) {
            if($index instanceof DocumentStore\Postgres\Metadata\MetadataColumnIndex) {
                foreach ($index->columns() as $column) {
                    $metadataColumns .= $column->sql().', ';
                }
                $indices[$i] = $index->indexCmd();
            }
        }

        $createSchemaCmd = "CREATE SCHEMA IF NOT EXISTS {$this->schemaName($collectionName)}";

        $cmd = <<<EOT
CREATE TABLE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)} (
    id {$this->docIdSchema},
    doc JSONB NOT NULL,
    $metadataColumns
    PRIMARY KEY (id)
);
EOT;

        $indicesCmds = array_map(function (Index $index) use ($collectionName) {
            return $this->indexToSqlCmd($index, $collectionName);
        }, $indices);

        $this->transactional(function() use ($createSchemaCmd, $cmd, $indicesCmds) {
            $this->connection->prepare($createSchemaCmd)->execute();
            $this->connection->prepare($cmd)->execute();

            array_walk($indicesCmds, function ($cmd) {
                $this->connection->prepare($cmd)->execute();
            });
        });
    }

    /**
     * @param string $collectionName
     * @throws \Throwable if dropping did not succeed
     */
    public function dropCollection(string $collectionName): void
    {
        $cmd = <<<EOT
DROP TABLE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)};
EOT;

        $this->transactional(function () use ($cmd) {
            $this->connection->prepare($cmd)->execute();
        });
    }

    public function hasCollectionIndex(string $collectionName, string $indexName): bool
    {
        $query = <<<EOT
SELECT INDEXNAME 
FROM pg_indexes
WHERE TABLENAME = '{$this->tableName($collectionName)}'
AND SCHEMANAME = '{$this->schemaName($collectionName)}'
AND INDEXNAME = '$indexName'
EOT;

        $stmt = $this->connection->prepare($query);

        $stmt->execute();

        $row = $stmt->fetchColumn();

        return !!$row;
    }

    /**
     * @param string $collectionName
     * @param Index $index
     * @throws \EventEngine\DocumentStore\Exception\RuntimeException if adding did not succeed
     */
    public function addCollectionIndex(string $collectionName, Index $index): void
    {
        $metadataColumnCmd = null;

        if($index instanceof DocumentStore\Postgres\Metadata\MetadataColumnIndex) {

            $columnsSql = '';

            foreach ($index->columns() as $column) {
                $columnsSql .= ', ADD COLUMN ' . $column->sql();
            }

            $columnsSql = substr($columnsSql, 2);

            $metadataColumnCmd = <<<EOT
ALTER TABLE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
    $columnsSql;
EOT;

            $index = $index->indexCmd();
        }

        $indexCmd = $this->indexToSqlCmd($index, $collectionName);

        $this->transactional(function() use ($metadataColumnCmd, $indexCmd) {

            if($metadataColumnCmd) {
                $this->connection->prepare($metadataColumnCmd)->execute();
            }

            $this->connection->prepare($indexCmd)->execute();
        });
    }

    /**
     * @param string $collectionName
     * @param string|Index $index
     * @throws \EventEngine\DocumentStore\Exception\RuntimeException if dropping did not succeed
     * @throws \Throwable
     */
    public function dropCollectionIndex(string $collectionName, $index): void
    {
        $metadataColumnCmd = null;

        if($index instanceof DocumentStore\Postgres\Metadata\MetadataColumnIndex) {

            $columnsSql = '';

            foreach ($index->columns() as $column) {
                $columnsSql .= ', DROP COLUMN IF EXISTS ' . $this->getColumnNameFromSql($column->sql());
            }

            $columnsSql = substr($columnsSql, 2);

            $metadataColumnCmd = <<<EOT
ALTER TABLE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
    $columnsSql;
EOT;
            $index = $index->indexCmd();
        }

        $indexName = is_string($index)? $index : $this->getIndexName($index);

        if($indexName === null) {
            throw new DocumentStore\Exception\RuntimeException("Given index does not have a name: ". VariableType::determine($index));
        }

        $cmd = "DROP INDEX $indexName";

        $this->transactional(function () use($cmd, $metadataColumnCmd) {
            $this->connection->prepare($cmd)->execute();

            if($metadataColumnCmd) {
                $this->connection->prepare($metadataColumnCmd)->execute();
            }
        });
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $doc
     * @throws \Throwable if adding did not succeed
     */
    public function addDoc(string $collectionName, string $docId, array $doc): void
    {
        $metadataKeysStr = '';
        $metadataValsStr = '';
        $metadata = [];

        if($this->useMetadataColumns && array_key_exists('metadata', $doc)) {
            $metadata = $doc['metadata'];
            unset($doc['metadata']);

            if(!is_array($metadata)) {
                throw new RuntimeException("metadata should be of type array");
            }

            foreach ($metadata as $k => $v) {
                $metadataKeysStr .= ', '.$k;
                $metadataValsStr .= ', :'.$k;
            }
        }

        $cmd = <<<EOT
INSERT INTO {$this->schemaName($collectionName)}.{$this->tableName($collectionName)} (
    id, doc{$metadataKeysStr}) VALUES (:id, :doc{$metadataValsStr}
);
EOT;

        $this->transactional(function () use ($cmd, $docId, $doc, $metadata) {
            $this->connection->prepare($cmd)->execute(array_merge([
                'id' => $docId,
                'doc' => json_encode($doc)
            ], $metadata));
        });
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws \Throwable if updating did not succeed
     */
    public function updateDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $metadataStr = '';
        $metadata = [];

        if($this->useMetadataColumns && array_key_exists('metadata', $docOrSubset)) {
            $metadata = $docOrSubset['metadata'];
            unset($docOrSubset['metadata']);


            foreach ($metadata as $k => $v) {
                $metadataStr .= ', '.$k.' = :'.$k;
            }
        }

        $cmd = <<<EOT
UPDATE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
SET doc = (to_jsonb(doc) || :doc){$metadataStr}
WHERE id = :id
;
EOT;
        $this->transactional(function () use ($cmd, $docId, $docOrSubset, $metadata) {
            $this->connection->prepare($cmd)->execute(array_merge([
                'id' => $docId,
                'doc' => json_encode($docOrSubset)
            ], $metadata));
        });
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param array $set
     * @throws \Throwable in case of connection error or other issues
     */
    public function updateMany(string $collectionName, Filter $filter, array $set): void
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $metadataStr = '';
        $metadata = [];

        if($this->useMetadataColumns && array_key_exists('metadata', $set)) {
            $metadata = $set['metadata'];
            unset($set['metadata']);


            foreach ($metadata as $k => $v) {
                $metadataStr .= ', '.$k.' = :'.$k;
            }
        }

        $cmd = <<<EOT
UPDATE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
SET doc = (to_jsonb(doc) || :doc){$metadataStr}
$where;
EOT;

        $args['doc'] = json_encode($set);
        $args = array_merge($args, $metadata);

        $this->transactional(function () use ($cmd, $args) {
            $this->connection->prepare($cmd)->execute($args);
        });
    }

    /**
     * Same as updateDoc except that doc is added to collection if it does not exist.
     *
     * @param string $collectionName
     * @param string $docId
     * @param array $docOrSubset
     * @throws \Throwable if insert/update did not succeed
     */
    public function upsertDoc(string $collectionName, string $docId, array $docOrSubset): void
    {
        $doc = $this->getDoc($collectionName, $docId);

        if($doc) {
            $this->updateDoc($collectionName, $docId, $docOrSubset);
        } else {
            $this->addDoc($collectionName, $docId, $docOrSubset);
        }
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @param array $doc
     * @throws \Throwable if updating did not succeed
     */
    public function replaceDoc(string $collectionName, string $docId, array $doc): void
    {
        $metadataStr = '';
        $metadata = [];

        if($this->useMetadataColumns && array_key_exists('metadata', $doc)) {
            $metadata = $doc['metadata'];
            unset($doc['metadata']);


            foreach ($metadata as $k => $v) {
                $metadataStr .= ', '.$k.' = :'.$k;
            }
        }

        $cmd = <<<EOT
UPDATE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
SET doc = :doc{$metadataStr}
WHERE id = :id
;
EOT;
        $this->transactional(function () use ($cmd, $docId, $doc, $metadata) {
            $this->connection->prepare($cmd)->execute(array_merge([
                'id' => $docId,
                'doc' => json_encode($doc)
           ], $metadata));
        });
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @param array $set
     * @throws \Throwable in case of connection error or other issues
     */
    public function replaceMany(string $collectionName, Filter $filter, array $set): void
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $metadataStr = '';
        $metadata = [];

        if($this->useMetadataColumns && array_key_exists('metadata', $set)) {
            $metadata = $set['metadata'];
            unset($set['metadata']);


            foreach ($metadata as $k => $v) {
                $metadataStr .= ', '.$k.' = :'.$k;
            }
        }

        $cmd = <<<EOT
UPDATE {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
SET doc = :doc{$metadataStr}
$where;
EOT;

        $args['doc'] = json_encode($set);
        $args = array_merge($args, $metadata);

        $this->transactional(function () use ($cmd, $args) {
            $this->connection->prepare($cmd)->execute($args);
        });
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @throws \Throwable if deleting did not succeed
     */
    public function deleteDoc(string $collectionName, string $docId): void
    {
        $cmd = <<<EOT
DELETE FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
WHERE id = :id
EOT;

        $this->transactional(function () use ($cmd, $docId) {
            $stmt = $this->connection->prepare($cmd);

            $stmt->execute(['id' => $docId]);
        });
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @throws \Throwable in case of connection error or other issues
     */
    public function deleteMany(string $collectionName, Filter $filter): void
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $cmd = <<<EOT
DELETE FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$where;
EOT;

        $this->transactional(function () use ($cmd, $args) {
            $this->connection->prepare($cmd)->execute($args);
        });
    }

    /**
     * @param string $collectionName
     * @param string $docId
     * @return array|null
     */
    public function getDoc(string $collectionName, string $docId): ?array
    {
        $query = <<<EOT
SELECT doc
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
WHERE id = :id
EOT;
        $stmt = $this->connection->prepare($query);

        $stmt->execute(['id' => $docId]);

        $row = $stmt->fetchColumn();

        if(!$row) {
            return null;
        }

        return json_decode($row, true);
    }

    /**
     * @inheritDoc
     */
    public function getPartialDoc(string $collectionName, PartialSelect $partialSelect, string $docId): ?array
    {
        $select = $this->makeSelect($partialSelect);

        $query = <<<EOT
SELECT $select
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
WHERE id = :id
EOT;
        $stmt = $this->connection->prepare($query);

        $stmt->execute(['id' => $docId]);

        $row = $stmt->fetch(\PDO::FETCH_ASSOC);

        if(!$row) {
            return null;
        }

        return $this->transformPartialDoc($partialSelect, $row);
    }

    /**
     * @inheritDoc
     */
    public function filterDocs(string $collectionName, Filter $filter, int $skip = null, int $limit = null, OrderBy $orderBy = null): \Traversable
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr ? "WHERE $filterStr" : '';

        $offset = $skip !== null ? "OFFSET $skip" : '';
        $limit = $limit !== null ? "LIMIT $limit" : '';

        $orderBy = $orderBy ? "ORDER BY " . implode(', ', $this->orderByToSort($orderBy)) : '';

        $query = <<<EOT
SELECT doc 
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$where
$orderBy
$limit
$offset;
EOT;
        $stmt = $this->connection->prepare($query);

        $stmt->execute($args);

        while($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            yield json_decode($row['doc'], true);
        }
    }

    /**
     * @inheritDoc
     */
    public function findDocs(string $collectionName, Filter $filter, int $skip = null, int $limit = null, OrderBy $orderBy = null): \Traversable
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr ? "WHERE $filterStr" : '';

        $offset = $skip !== null ? "OFFSET $skip" : '';
        $limit = $limit !== null ? "LIMIT $limit" : '';

        $orderBy = $orderBy ? "ORDER BY " . implode(', ', $this->orderByToSort($orderBy)) : '';

        $query = <<<EOT
SELECT id, doc 
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$where
$orderBy
$limit
$offset;
EOT;
        $stmt = $this->connection->prepare($query);

        $stmt->execute($args);

        while($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            yield $row['id'] => json_decode($row['doc'], true);
        }
    }

    public function findPartialDocs(string $collectionName, PartialSelect $partialSelect, Filter $filter, int $skip = null, int $limit = null, OrderBy $orderBy = null): \Traversable
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $select = $this->makeSelect($partialSelect);

        $where = $filterStr ? "WHERE $filterStr" : '';

        $offset = $skip !== null ? "OFFSET $skip" : '';
        $limit = $limit !== null ? "LIMIT $limit" : '';

        $orderBy = $orderBy ? "ORDER BY " . implode(', ', $this->orderByToSort($orderBy)) : '';

        $query = <<<EOT
SELECT $select 
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$where
$orderBy
$limit
$offset;
EOT;

        $stmt = $this->connection->prepare($query);

        $stmt->execute($args);

        while($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            yield $row[self::PARTIAL_SELECT_DOC_ID] => $this->transformPartialDoc($partialSelect, $row);
        }
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @return array
     */
    public function filterDocIds(string $collectionName, Filter $filter): array
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr ? "WHERE {$filterStr}" : '';
        $query = "SELECT id FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)} {$where}";

        $stmt = $this->connection->prepare($query);
        $stmt->execute($args);

        $docIds = [];
        while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
            $docIds[] = $row['id'];
        }

        return $docIds;
    }

    /**
     * @param string $collectionName
     * @param Filter $filter
     * @return int number of docs
     */
    public function countDocs(string $collectionName, Filter $filter): int
    {
        [$filterStr, $args] = $this->filterProcessor->process($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $query = <<<EOT
SELECT count(doc) 
FROM {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$where;
EOT;
        $stmt = $this->connection->prepare($query);

        $stmt->execute($args);

        return (int) $stmt->fetchColumn(0);
    }

    private function transactional(callable $callback)
    {
        if($this->manageTransactions) {
            $this->connection->beginTransaction();
        }

        try {
            $callback();
            if($this->manageTransactions) {
                $this->connection->commit();
            }
        } catch (\Throwable $exception) {
            if($this->manageTransactions) {
                $this->connection->rollBack();
            }
            throw $exception;
        }
    }

    private function propToJsonPath(string $field): string
    {
        if($this->useMetadataColumns && strpos($field, 'metadata.') === 0) {
            return str_replace('metadata.', '', $field);
        }

        return "doc->'" . str_replace('.', "'->'", $field) . "'";
    }

    private function makeSelect(PartialSelect $partialSelect): string
    {
        $select = 'id as "'.self::PARTIAL_SELECT_DOC_ID.'", ';

        foreach ($partialSelect->fieldAliasMap() as $mapItem) {

            if($mapItem['alias'] === self::PARTIAL_SELECT_DOC_ID) {
                throw new RuntimeException(sprintf(
                    "Invalid select alias. You cannot use %s as alias, because it is reserved for internal use",
                    self::PARTIAL_SELECT_DOC_ID
                ));
            }

            if($mapItem['alias'] === self::PARTIAL_SELECT_MERGE) {
                throw new RuntimeException(sprintf(
                    "Invalid select alias. You cannot use %s as alias, because it is reserved for internal use",
                    self::PARTIAL_SELECT_MERGE
                ));
            }

            if($mapItem['alias'] === PartialSelect::MERGE_ALIAS) {
                $mapItem['alias'] = self::PARTIAL_SELECT_MERGE;
            }

            $select.= $this->propToJsonPath($mapItem['field']) . ' as "' . $mapItem['alias'] . '", ';
        }

        $select = mb_substr($select, 0, mb_strlen($select) - 2);

        return $select;
    }

    private function transformPartialDoc(PartialSelect $partialSelect, array $selectedDoc): array
    {
        $partialDoc = [];

        foreach ($partialSelect->fieldAliasMap() as ['field' => $field, 'alias' => $alias]) {
            if($alias === PartialSelect::MERGE_ALIAS) {
                if(null === $selectedDoc[self::PARTIAL_SELECT_MERGE] ?? null) {
                    continue;
                }

                $value = json_decode($selectedDoc[self::PARTIAL_SELECT_MERGE], true);

                if(!is_array($value)) {
                    throw new RuntimeException('Merge not possible. $merge alias was specified for field: ' . $field . ' but field value is not an array: ' . json_encode($value));
                }

                foreach ($value as $k => $v) {
                    $partialDoc[$k] = $v;
                }

                continue;
            }

            $value = $selectedDoc[$alias] ?? null;

            if(is_string($value)) {
                $value = json_decode($value, true);
            }

            $keys = explode('.', $alias);

            $ref = &$partialDoc;
            foreach ($keys as $i => $key) {
                if(!array_key_exists($key, $ref)) {
                    $ref[$key] = [];
                }
                $ref = &$ref[$key];
            }
            $ref = $value;
            unset($ref);
        }

        return $partialDoc;
    }

    private function orderByToSort(DocumentStore\OrderBy\OrderBy $orderBy): array
    {
        $sort = [];

        if($orderBy instanceof DocumentStore\OrderBy\AndOrder) {
            /** @var DocumentStore\OrderBy\Asc|DocumentStore\OrderBy\Desc $orderByA */
            $orderByA = $orderBy->a();
            $direction = $orderByA instanceof DocumentStore\OrderBy\Asc ? 'ASC' : 'DESC';
            $prop = $this->propToJsonPath($orderByA->prop());
            $sort[] = "{$prop} $direction";

            $sortB = $this->orderByToSort($orderBy->b());

            return array_merge($sort, $sortB);
        }

        /** @var DocumentStore\OrderBy\Asc|DocumentStore\OrderBy\Desc $orderBy */
        $direction = $orderBy instanceof DocumentStore\OrderBy\Asc ? 'ASC' : 'DESC';
        $prop = $this->propToJsonPath($orderBy->prop());
        return ["{$prop} $direction"];
    }

    private function indexToSqlCmd(Index $index, string $collectionName): string
    {
        if($index instanceof DocumentStore\FieldIndex) {
            $type = $index->unique() ? 'UNIQUE INDEX' : 'INDEX';
            $fields = '('.$this->extractFieldPartFromFieldIndex($index).')';
        } elseif ($index instanceof DocumentStore\MultiFieldIndex) {
            $type = $index->unique() ? 'UNIQUE INDEX' : 'INDEX';
            $fieldParts = array_map([$this, 'extractFieldPartFromFieldIndex'], $index->fields());
            $fields = '('.implode(', ', $fieldParts).')';
        } elseif ($index instanceof DocumentStore\Postgres\Index\RawSqlIndexCmd) {
            return $index->sql();
        } else {
            throw new RuntimeException('Unsupported index type. Got ' . get_class($index));
        }

        $name = $index->name() ?? '';

        $cmd = <<<EOT
CREATE $type $name ON {$this->schemaName($collectionName)}.{$this->tableName($collectionName)}
$fields;
EOT;

        return $cmd;
    }

    private function getIndexName(Index $index): ?string
    {
        if(method_exists($index, 'name')) {
            return $index->name();
        }

        return null;
    }

    private function getColumnNameFromSql(string $columnSql): string
    {
        $parts = explode(' ', $columnSql);

        return $parts[0];
    }

    private function extractFieldPartFromFieldIndex(DocumentStore\FieldIndex $fieldIndex): string
    {
        $direction = $fieldIndex->sort() === Index::SORT_ASC ? 'ASC' : 'DESC';
        $prop = $this->propToJsonPath($fieldIndex->field());
        return "($prop) $direction";
    }

    private function tableName(string $collectionName): string
    {
        if (false !== $dotPosition = strpos($collectionName, '.')) {
            $collectionName = substr($collectionName, $dotPosition+1);
        }

        return mb_strtolower($this->tablePrefix . $collectionName);
    }

    private function schemaName(string $collectionName): string
    {
        $schemaName = 'public';
        if (false !== $dotPosition = strpos($collectionName, '.')) {
            $schemaName = substr($collectionName, 0, $dotPosition);
        }
        return mb_strtolower($schemaName);
    }
}
