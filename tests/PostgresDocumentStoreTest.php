<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStoreTest\Postgres;

use EventEngine\DocumentStore\Filter\AnyOfDocIdFilter;
use EventEngine\DocumentStore\Filter\AnyOfFilter;
use EventEngine\DocumentStore\Filter\DocIdFilter;
use EventEngine\DocumentStore\Filter\NotFilter;
use PHPUnit\Framework\TestCase;
use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\MultiFieldIndex;
use EventEngine\DocumentStore\Postgres\PostgresDocumentStore;
use Ramsey\Uuid\Uuid;

class PostgresDocumentStoreTest extends TestCase
{
    private CONST TABLE_PREFIX = 'test_';

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
        $this->documentStore = new PostgresDocumentStore($this->connection, self::TABLE_PREFIX);
    }

    public function tearDown(): void
    {
        TestUtil::tearDownDatabase();
    }

    /**
     * @test
     */
    public function it_adds_collection(): void
    {
        $this->documentStore->addCollection('test');
        $this->assertTrue($this->documentStore->hasCollection('test'));
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_asc(): void
    {
        $collectionName = 'test_field_index_asc';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_asc'));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (((doc -> 'field_asc'::text)))", $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_desc(): void
    {
        $collectionName = 'test_field_index_desc';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_desc', Index::SORT_DESC));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (((doc -> 'field_desc'::text)) DESC)", $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_field_index_unique(): void
    {
        $collectionName = 'test_field_index_unique';
        $this->documentStore->addCollection($collectionName, FieldIndex::forField('field_asc', Index::SORT_DESC, true));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith("USING btree (((doc -> 'field_asc'::text)) DESC)", $indexes[1]['indexdef']);
        $this->assertStringStartsWith('CREATE UNIQUE INDEX', $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_adds_collection_with_multi_field_index_asc(): void
    {
        $collectionName = 'test_multi_field_index_asc';
        $this->documentStore->addCollection($collectionName, MultiFieldIndex::forFields(['a', 'b']));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith(
            "USING btree (((doc -> 'a'::text)), ((doc -> 'b'::text)))",
            $indexes[1]['indexdef']
        );
    }

    /**
     * @test
     */
    public function it_adds_collection_with_multi_field_index_unique(): void
    {
        $collectionName = 'test_multi_field_index_unique';
        $this->documentStore->addCollection($collectionName, MultiFieldIndex::forFields(['a', 'b'], true));
        $this->assertTrue($this->documentStore->hasCollection($collectionName));

        $indexes = $this->getIndexes($collectionName);

        $this->assertCount(2, $indexes);
        $this->assertStringEndsWith(
            "USING btree (((doc -> 'a'::text)), ((doc -> 'b'::text)))",
            $indexes[1]['indexdef']
        );
        $this->assertStringStartsWith('CREATE UNIQUE INDEX', $indexes[1]['indexdef']);
    }

    /**
     * @test
     */
    public function it_handles_named_indices(): void
    {
        $collectionName = 'test_named_indices';

        $this->documentStore->addCollection(
            $collectionName,
            FieldIndex::namedIndexForField('testidx_field_a', 'a'),
            MultiFieldIndex::namedIndexForFields('multitestidx_fields_a_b', ['a', 'b'])
        );

        $this->assertTrue($this->documentStore->hasCollectionIndex($collectionName, 'testidx_field_a'));

        $this->assertTrue($this->documentStore->hasCollectionIndex($collectionName, 'multitestidx_fields_a_b'));

        $this->documentStore->dropCollectionIndex($collectionName, 'testidx_field_a');

        $this->assertFalse($this->documentStore->hasCollectionIndex($collectionName, 'testidx_field_a'));

        $this->documentStore->addCollectionIndex($collectionName, FieldIndex::namedIndexForField('testidx_field_b', 'b'));

        $this->assertTrue($this->documentStore->hasCollectionIndex($collectionName, 'testidx_field_b'));
    }

    /**
     * @test
     */
    public function it_handles_any_of_filter()
    {
        $collectionName = 'test_any_of_filter';
        $this->documentStore->addCollection($collectionName);

        $doc1 = ["foo" => "bar"];
        $doc2 = ["foo" => "baz"];
        $doc3 = ["foo" => "bat"];

        $docs = [$doc1, $doc2, $doc3];

        array_walk($docs, function (array $doc) use ($collectionName) {
            $this->documentStore->addDoc($collectionName, Uuid::uuid4()->toString(), $doc);
        });

        $filteredDocs = $this->documentStore->filterDocs(
            $collectionName,
            new AnyOfFilter("foo", ["bar", "bat"])
        );

        $this->assertCount(2, $filteredDocs);
    }

    /**
     * @test
     */
    public function it_handles_not_any_of_filter()
    {
        $collectionName = 'test_not_any_of_filter';
        $this->documentStore->addCollection($collectionName);

        $doc1 = ["foo" => "bar"];
        $doc2 = ["foo" => "baz"];
        $doc3 = ["foo" => "bat"];

        $docs = [$doc1, $doc2, $doc3];

        array_walk($docs, function (array $doc) use ($collectionName) {
            $this->documentStore->addDoc($collectionName, Uuid::uuid4()->toString(), $doc);
        });

        $filteredDocs = $this->documentStore->filterDocs(
            $collectionName,
            new NotFilter(new AnyOfFilter("foo", ["bar", "bat"]))
        );

        $filteredDocs = iterator_to_array($filteredDocs);

        $this->assertCount(1, $filteredDocs);

        $this->assertSame('baz', $filteredDocs[0]['foo']);
    }

    /**
     * @test
     */
    public function it_handles_doc_id_filter()
    {
        $collectionName = 'test_doc_id_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => 'bar']);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => 'bat']);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new DocIdFilter($secondDocId)
        ));

        $this->assertCount(1, $filteredDocs);

        $this->assertSame('bat', $filteredDocs[0]['foo']);
    }

    /**
     * @test
     */
    public function it_handles_any_of_doc_id_filter()
    {
        $collectionName = 'test_any_of_doc_id_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => 'bar']);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => 'bat']);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => 'baz']);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new AnyOfDocIdFilter([$firstDocId, $thirdDocId])
        ));

        $this->assertCount(2, $filteredDocs);

        $vals = array_map(function (array $doc) {
            return $doc['foo'];
        }, $filteredDocs);

        $this->assertEquals(['bar', 'baz'], $vals);
    }

    /**
     * @test
     */
    public function it_handles_not_any_of_id_filter()
    {
        $collectionName = 'test_any_of_doc_id_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => 'bar']);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => 'bat']);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => 'baz']);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new NotFilter(new AnyOfDocIdFilter([$firstDocId, $thirdDocId]))
        ));

        $this->assertCount(1, $filteredDocs);

        $vals = array_map(function (array $doc) {
            return $doc['foo'];
        }, $filteredDocs);

        $this->assertEquals(['bat'], $vals);
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
