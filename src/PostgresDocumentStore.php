<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019 prooph software GmbH <contact@prooph.de>
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
use EventEngine\DocumentStore\Postgres\Exception\InvalidArgumentException;
use EventEngine\DocumentStore\Postgres\Exception\RuntimeException;

final class PostgresDocumentStore implements DocumentStore\DocumentStore
{
    /**
     * @var \PDO
     */
    private $connection;

    private $tablePrefix = 'em_ds_';

    private $docIdSchema = 'UUID NOT NULL';

    private $manageTransactions;

    public function __construct(
        \PDO $connection,
        string $tablePrefix = null,
        string $docIdSchema = null,
        bool $transactional = true
    ) {
        $this->connection = $connection;
        $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        if(null !== $tablePrefix) {
            $this->tablePrefix = $tablePrefix;
        }

        if(null !== $docIdSchema) {
            $this->docIdSchema = $docIdSchema;
        }

        $this->manageTransactions = $transactional;
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
        $cmd = <<<EOT
CREATE TABLE {$this->tableName($collectionName)} (
    id {$this->docIdSchema},
    doc JSONB NOT NULL,
    PRIMARY KEY (id)
);
EOT;

        $indicesCmds = array_map(function (Index $index) use ($collectionName) {
            return $this->indexToSqlCmd($index, $collectionName);
        }, $indices);

        $this->transactional(function() use ($cmd, $indicesCmds) {
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
DROP TABLE {$this->tableName($collectionName)};
EOT;

        $this->transactional(function () use ($cmd) {
            $this->connection->prepare($cmd)->execute();
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
        $cmd = <<<EOT
INSERT INTO {$this->tableName($collectionName)} (id, doc) VALUES (:id, :doc);
EOT;
        $this->transactional(function () use ($cmd, $docId, $doc) {
            $this->connection->prepare($cmd)->execute([
                'id' => $docId,
                'doc' => json_encode($doc)
            ]);
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
        $cmd = <<<EOT
UPDATE {$this->tableName($collectionName)}
SET doc = (to_jsonb(doc) || :doc)
WHERE id = :id
;
EOT;
        $this->transactional(function () use ($cmd, $docId, $docOrSubset) {
            $this->connection->prepare($cmd)->execute([
                'id' => $docId,
                'doc' => json_encode($docOrSubset)
            ]);
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
        [$filterStr, $args] = $this->filterToWhereClause($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $cmd = <<<EOT
UPDATE {$this->tableName($collectionName)}
SET doc = (to_jsonb(doc) || :doc)
$where;
EOT;

        $args['doc'] = json_encode($set);

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
     * @throws \Throwable if deleting did not succeed
     */
    public function deleteDoc(string $collectionName, string $docId): void
    {
        $cmd = <<<EOT
DELETE FROM {$this->tableName($collectionName)}
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
        [$filterStr, $args] = $this->filterToWhereClause($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $cmd = <<<EOT
DELETE FROM {$this->tableName($collectionName)}
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
FROM {$this->tableName($collectionName)}
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
     * @param string $collectionName
     * @param Filter $filter
     * @param int|null $skip
     * @param int|null $limit
     * @param OrderBy|null $orderBy
     * @return \Traversable list of docs
     */
    public function filterDocs(string $collectionName, Filter $filter, int $skip = null, int $limit = null, OrderBy $orderBy = null): \Traversable
    {
        [$filterStr, $args] = $this->filterToWhereClause($filter);

        $where = $filterStr? "WHERE $filterStr" : '';

        $offset = $skip !== null ? "OFFSET $skip" : '';
        $limit = $limit !== null ? "LIMIT $limit" : '';

        $orderBy = $orderBy ? "ORDER BY " . implode(', ', $this->orderByToSort($orderBy)) : '';

        $query = <<<EOT
SELECT doc 
FROM {$this->tableName($collectionName)}
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

    private function filterToWhereClause(Filter $filter, $argsCount = 0): array
    {
        if($filter instanceof DocumentStore\Filter\AnyFilter) {
            if($argsCount > 0) {
                throw new InvalidArgumentException('AnyFilter cannot be used together with other filters.');
            }
            return [null, [], $argsCount];
        }

        if($filter instanceof DocumentStore\Filter\AndFilter) {
            [$filterA, $argsA, $argsCount] = $this->filterToWhereClause($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->filterToWhereClause($filter->bFilter(), $argsCount);
            return ["($filterA AND $filterB)", array_merge($argsA, $argsB), $argsCount];
        }

        if($filter instanceof DocumentStore\Filter\OrFilter) {
            [$filterA, $argsA, $argsCount] = $this->filterToWhereClause($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->filterToWhereClause($filter->bFilter(), $argsCount);
            return ["($filterA OR $filterB)", array_merge($argsA, $argsB), $argsCount];
        }

        switch (get_class($filter)) {
            case DocumentStore\Filter\EqFilter::class:
                /** @var DocumentStore\Filter\EqFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop = :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\GtFilter::class:
                /** @var DocumentStore\Filter\GtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop > :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\GteFilter::class:
                /** @var DocumentStore\Filter\GteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop >= :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\LtFilter::class:
                /** @var DocumentStore\Filter\LtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop < :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\LteFilter::class:
                /** @var DocumentStore\Filter\LteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop <= :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\LikeFilter::class:
                /** @var DocumentStore\Filter\LikeFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                $propParts = explode('->', $prop);
                $lastProp = array_pop($propParts);
                $prop = implode('->', $propParts) . '->>'.$lastProp;
                return ["$prop LIKE :a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\NotFilter::class:
                /** @var DocumentStore\Filter\NotFilter $filter */
                $innerFilter = $filter->innerFilter();

                if (!$this->isPropFilter($innerFilter)) {
                    throw new RuntimeException('Not filter cannot be combined with a non prop filter!');
                }

                [$innerFilterStr, $args, $argsCount] = $this->filterToWhereClause($innerFilter);

                return ["NOT $innerFilterStr", $args, $argsCount];
            case DocumentStore\Filter\InArrayFilter::class:
                /** @var DocumentStore\Filter\InArrayFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop @> :a$argsCount", ["a$argsCount" => json_encode($filter->val())], ++$argsCount];
            case DocumentStore\Filter\ExistsFilter::class:
                /** @var DocumentStore\Filter\ExistsFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                $propParts = explode('->', $prop);
                $lastProp = trim(array_pop($propParts), "'");
                $parentProps = implode('->', $propParts);
                return ["JSONB_EXISTS($parentProps, '$lastProp')", [], $argsCount];
            default:
                throw new RuntimeException('Unsupported filter type. Got ' . get_class($filter));
        }
    }

    private function propToJsonPath(string $field): string
    {
        return "doc->'" . str_replace('.', "'->'", $field) . "'";
    }

    private function isPropFilter(Filter $filter): bool
    {
        switch (get_class($filter)) {
            case DocumentStore\Filter\AndFilter::class:
            case DocumentStore\Filter\OrFilter::class:
            case DocumentStore\Filter\NotFilter::class:
                return false;
            default:
                return true;
        }
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
        } else {
            throw new RuntimeException('Unsupported index type. Got ' . get_class($index));
        }

        $cmd = <<<EOT
CREATE $type ON {$this->tableName($collectionName)}
$fields;
EOT;

        return $cmd;
    }

    private function extractFieldPartFromFieldIndex(DocumentStore\FieldIndex $fieldIndex): string
    {
        $direction = $fieldIndex->sort() === Index::SORT_ASC ? 'ASC' : 'DESC';
        $prop = $this->propToJsonPath($fieldIndex->field());
        return "($prop) $direction";
    }

    private function tableName(string $collectionName): string
    {
        return mb_strtolower($this->tablePrefix . $collectionName);
    }
}
