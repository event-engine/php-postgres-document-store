<?php
declare(strict_types=1);

namespace EventEngine\DocumentStoreTest\Postgres;

use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Filter\AnyFilter;
use EventEngine\DocumentStore\Filter\EqFilter;
use EventEngine\DocumentStore\Filter\GteFilter;
use EventEngine\DocumentStore\Filter\GtFilter;
use EventEngine\DocumentStore\Filter\LtFilter;
use EventEngine\DocumentStore\MultiFieldIndex;
use EventEngine\DocumentStore\OrderBy\Asc;
use EventEngine\DocumentStore\OrderBy\Desc;
use EventEngine\DocumentStore\Postgres\Index\RawSqlIndexCmd;
use EventEngine\DocumentStore\Postgres\Metadata\Column;
use EventEngine\DocumentStore\Postgres\Metadata\MetadataColumnIndex;
use EventEngine\DocumentStore\Postgres\PostgresDocumentStore;
use PHPUnit\Framework\TestCase;
use Ramsey\Uuid\Uuid;

final class MetadataPostgresDocumentStoreTest extends TestCase
{
    private CONST TABLE_PREFIX = 'metatest_';

    /**
     * @var PostgresDocumentStore
     */
    protected $documentStore;

    /**
     * @var \PDO
     */
    protected $connection;

    protected function setUp(): void
    {
        $this->connection = TestUtil::getConnection();
        $this->documentStore = new PostgresDocumentStore($this->connection, self::TABLE_PREFIX, null, true, true);
    }

    public function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_adds_collection_with_metadata_column_index()
    {
        $collectionName = 'test_collection_with_metadata_column_index';
        $tablePrefix = self::TABLE_PREFIX;
        $metadataColumnIndex = new MetadataColumnIndex(
            new RawSqlIndexCmd("CREATE INDEX test_column_index ON {$tablePrefix}{$collectionName}(version)", 'test_column_index'),
            new Column('version INTEGER')
        );

        $this->documentStore->addCollection($collectionName, $metadataColumnIndex);

        $columns = $this->getColumns($collectionName);

        $this->assertCount(3, $columns);
        $this->assertEquals('version', $columns[2]);

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (version)", $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_adds_metadata_index_to_existing_collection()
    {
        $collectionName = 'test_collection_with_altered_metadata_column_index';

        $this->documentStore->addCollection($collectionName);

        $index = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $this->documentStore->addCollectionIndex($collectionName, $index);

        $columns = $this->getColumns($collectionName);

        $this->assertCount(3, $columns);
        $this->assertEquals('version', $columns[2]);

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (version)", $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_drops_metadata_index_and_column()
    {
        $collectionName = 'test_collection_with_dropped_metadata_column_index';

        $index = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $this->documentStore->addCollection($collectionName, $index);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(3, $columns);

        $this->documentStore->dropCollectionIndex($collectionName, $index);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(2, $columns);
        $this->assertEquals(['id', 'doc'], $columns);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_mulitple_metadata_columns()
    {
        $collectionName = 'test_collection_with_multi_metadata_column_index';

        $index1 = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $index2 = new MetadataColumnIndex(
            MultiFieldIndex::namedIndexForFields('multi_meta_idx_stars_downloads', ['metadata.stars', 'metadata.downloads']),
            new Column('stars INTEGER'),
            new Column('downloads INTEGER')
        );

        $this->documentStore->addCollection($collectionName, $index1, $index2);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(5, $columns);
        $this->assertEquals(['id', 'doc', 'version', 'stars', 'downloads'], $columns);

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(3, $indexes);
        $this->assertStringEndsWith("USING btree (version)", $indexes[1]['indexdef']);
        $this->assertStringEndsWith("USING btree (stars, downloads)", $indexes[2]['indexdef']);
    }

    /**
     * @test
     */
    public function it_adds_multiple_metadata_indexes_to_collection()
    {
        $collectionName = 'test_collection_with_altered_metadata_column_index';

        $this->documentStore->addCollection($collectionName);

        $index1 = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $this->documentStore->addCollectionIndex($collectionName, $index1);

        $index2 = new MetadataColumnIndex(
            MultiFieldIndex::namedIndexForFields('multi_meta_idx_stars_downloads', ['metadata.stars', 'metadata.downloads']),
            new Column('stars INTEGER'),
            new Column('downloads INTEGER')
        );

        $this->documentStore->addCollectionIndex($collectionName, $index2);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(5, $columns);
        $this->assertEquals(['id', 'doc', 'version', 'stars', 'downloads'], $columns);

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(3, $indexes);
        $this->assertStringEndsWith("USING btree (version)", $indexes[1]['indexdef']);
        $this->assertStringEndsWith("USING btree (stars, downloads)", $indexes[2]['indexdef']);
    }

    /**
     * @test
     */
    public function it_drops_multi_column_metadata_index()
    {
        $collectionName = 'test_collection_with_dropped_multi_column_index';

        $index1 = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $index2 = new MetadataColumnIndex(
            MultiFieldIndex::namedIndexForFields('multi_meta_idx_stars_downloads', ['metadata.stars', 'metadata.downloads']),
            new Column('stars INTEGER'),
            new Column('downloads INTEGER')
        );

        $this->documentStore->addCollection($collectionName, $index1, $index2);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(5, $columns);

        $this->documentStore->dropCollectionIndex($collectionName, $index2);

        $columns = $this->getColumns($collectionName);
        $this->assertCount(3, $columns);
        $this->assertEquals('version', $columns[2]);

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (version)", $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_fills_and_queries_metadata_column()
    {
        $collectionName = 'test_col_query_version_meta';

        $index1 = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_version', 'metadata.version'),
            new Column('version INTEGER')
        );

        $this->documentStore->addCollection($collectionName, $index1);

        $docId1 = Uuid::uuid4()->toString();
        $docId2 = Uuid::uuid4()->toString();
        $docId3 = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $docId1, ['state' => ['name' => 'v1'], 'metadata' => ['version' => 1]]);
        $this->documentStore->addDoc($collectionName, $docId2, ['state' => ['name' => 'v2'], 'metadata' => ['version' => 2]]);
        $this->documentStore->addDoc($collectionName, $docId3, ['state' => ['name' => 'v3'], 'metadata' => ['version' => 3]]);

        $prefix = self::TABLE_PREFIX;
        $stmt = "SELECT * FROM $prefix{$collectionName} WHERE version = 2;";
        $stmt = $this->connection->prepare($stmt);
        $stmt->execute();
        $docs = $stmt->fetchAll();

        $this->assertCount(1, $docs);
        $this->assertEquals($docId2, $docs[0]['id']);
        $this->assertEquals(['state' => ['name' => 'v2']], json_decode($docs[0]['doc'], true));
        $this->assertEquals(2, $docs[0]['version']);

        $docs = iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new GteFilter('metadata.version', 2),
            null,
            null,
            Desc::byProp('metadata.version')
        ));

        $this->assertCount(2, $docs);
        $this->assertEquals('v3', $docs[0]['state']['name']);
        $this->assertEquals('v2', $docs[1]['state']['name']);


        $this->documentStore->updateDoc($collectionName, $docId1, ['state' => ['name' => 'v4'], 'metadata' => ['version' => 4]]);

        $docs = iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new GteFilter('metadata.version', 2),
            null,
            null,
            Desc::byProp('metadata.version')
        ));

        $this->assertCount(3, $docs);
        $this->assertEquals('v4', $docs[0]['state']['name']);

        $this->documentStore->updateMany(
            $collectionName,
            new LtFilter('metadata.version', 4),
            [
                'state' => ['name' => 'v5'],
                'metadata' => ['version' => 5]
            ]
        );

        $docs = iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new GtFilter('metadata.version', 4),
            null,
            null,
            Desc::byProp('metadata.version')
        ));

        $this->assertCount(2, $docs);
        $this->assertEquals('v5', $docs[0]['state']['name']);
        $this->assertEquals('v5', $docs[1]['state']['name']);

        $this->documentStore->deleteMany($collectionName, new EqFilter('metadata.version', 5));

        $docs = iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new AnyFilter(),
            null,
            null,
            Desc::byProp('metadata.version')
        ));

        $this->assertCount(1, $docs);
        $this->assertEquals('v4', $docs[0]['state']['name']);
    }

    /**
     * @test
     */
    public function it_fills_and_queries_metadata_varchar_column()
    {
        $collectionName = 'test_col_query_name_meta';

        $index1 = new MetadataColumnIndex(
            FieldIndex::namedIndexForField('meta_field_idx_name', 'metadata.name'),
            new Column('name VARCHAR(10)')
        );

        $this->documentStore->addCollection($collectionName, $index1);

        $docId1 = Uuid::uuid4()->toString();
        $docId2 = Uuid::uuid4()->toString();
        $docId3 = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $docId1, ['state' => ['name' => 'v1'], 'metadata' => ['name' => 'v1']]);
        $this->documentStore->addDoc($collectionName, $docId2, ['state' => ['name' => 'v2'], 'metadata' => ['name' => 'v2']]);
        $this->documentStore->addDoc($collectionName, $docId3, ['state' => ['name' => 'v3'], 'metadata' => ['name' => 'v3']]);

        $prefix = self::TABLE_PREFIX;
        $stmt = "SELECT * FROM $prefix{$collectionName} WHERE name = 'v2';";
        $stmt = $this->connection->prepare($stmt);
        $stmt->execute();
        $docs = $stmt->fetchAll();

        $this->assertCount(1, $docs);
        $this->assertEquals($docId2, $docs[0]['id']);
        $this->assertEquals(['state' => ['name' => 'v2']], json_decode($docs[0]['doc'], true));
        $this->assertEquals('v2', $docs[0]['name']);

        $docs = iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new GteFilter('metadata.name', 'v2'),
            null,
            null,
            Desc::byProp('metadata.name')
        ));

        $this->assertCount(2, $docs);
        $this->assertEquals('v3', $docs[0]['state']['name']);
        $this->assertEquals('v2', $docs[1]['state']['name']);
    }

    private function getIndexes(string $collectionName): array
    {
        return TestUtil::getIndexes($this->connection, self::TABLE_PREFIX.$collectionName);
    }

    private function getColumns(string $collectionName): array
    {
        return TestUtil::getColumns($this->connection, self::TABLE_PREFIX.$collectionName);
    }
}
