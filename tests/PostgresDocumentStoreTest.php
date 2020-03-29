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

use EventEngine\DocumentStore\Filter\AndFilter;
use EventEngine\DocumentStore\Filter\AnyFilter;
use EventEngine\DocumentStore\Filter\AnyOfDocIdFilter;
use EventEngine\DocumentStore\Filter\AnyOfFilter;
use EventEngine\DocumentStore\Filter\DocIdFilter;
use EventEngine\DocumentStore\Filter\EqFilter;
use EventEngine\DocumentStore\Filter\GtFilter;
use EventEngine\DocumentStore\Filter\InArrayFilter;
use EventEngine\DocumentStore\Filter\LtFilter;
use EventEngine\DocumentStore\Filter\NotFilter;
use EventEngine\DocumentStore\Filter\OrFilter;
use EventEngine\DocumentStore\PartialSelect;
use PHPUnit\Framework\TestCase;
use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\MultiFieldIndex;
use EventEngine\DocumentStore\Postgres\PostgresDocumentStore;
use Ramsey\Uuid\Uuid;
use function array_map;
use function array_walk;
use function iterator_to_array;

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
    public function it_uses_doc_ids_as_iterator_keys()
    {
        $collectionName = 'test_any_of_filter';
        $this->documentStore->addCollection($collectionName);

        $doc1 = ['id' => Uuid::uuid4()->toString(), 'doc' => ["foo" => "bar"]];
        $doc2 = ['id' => Uuid::uuid4()->toString(), 'doc' => ["foo" => "baz"]];
        $doc3 = ['id' => Uuid::uuid4()->toString(), 'doc' => ["foo" => "bat"]];

        $docs = [$doc1, $doc2, $doc3];

        array_walk($docs, function (array $doc) use ($collectionName) {
            $this->documentStore->addDoc($collectionName, $doc['id'], $doc['doc']);
        });

        $filteredDocs = iterator_to_array($this->documentStore->findDocs(
            $collectionName,
            new AnyOfFilter("foo", ["bar", "bat"])
        ));

        $this->assertEquals([
            $doc1['id'] => $doc1['doc'],
            $doc3['id'] => $doc3['doc']
        ], $filteredDocs);
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

    /**
     * @test
     */
    public function it_handles_in_array_filter()
    {
        $collectionName = 'test_in_array_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => ['bar' => ['tag1', 'tag2'], 'ref' => $firstDocId]]);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => ['bar' => ['tag2', 'tag3'], 'ref' => $secondDocId]]);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => ['bar' => ['tag3', 'tag4'], 'ref' => $thirdDocId]]);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new InArrayFilter('foo.bar', 'tag3')
        ));

        $this->assertCount(2, $filteredDocs);

        $refs = array_map(function (array $doc) {
            return $doc['foo']['ref'];
        }, $filteredDocs);

        $this->assertEquals([$secondDocId, $thirdDocId], $refs);
    }

    /**
     * @test
     */
    public function it_handles_not_in_array_filter()
    {
        $collectionName = 'test_not_in_array_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => ['bar' => ['tag1', 'tag2'], 'ref' => $firstDocId]]);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => ['bar' => ['tag2', 'tag3'], 'ref' => $secondDocId]]);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => ['bar' => ['tag3', 'tag4'], 'ref' => $thirdDocId]]);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new NotFilter(new InArrayFilter('foo.bar', 'tag3'))
        ));

        $this->assertCount(1, $filteredDocs);

        $refs = array_map(function (array $doc) {
            return $doc['foo']['ref'];
        }, $filteredDocs);

        $this->assertEquals([$firstDocId], $refs);
    }

    /**
     * @test
     */
    public function it_handles_in_array_filter_with_object_items()
    {
        $collectionName = 'test_in_array_with_object_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => ['bar' => [['tag' => 'tag1', 'other' => 'data'], ['tag' => 'tag2']], 'ref' => $firstDocId]]);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => ['bar' => [['tag' => 'tag2', 'other' => 'data'], ['tag' => 'tag3']], 'ref' => $secondDocId]]);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => ['bar' => [['tag' => 'tag3', 'other' => 'data'], ['tag' => 'tag4']], 'ref' => $thirdDocId]]);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new InArrayFilter('foo.bar', ['tag' => 'tag3'])
        ));

        $this->assertCount(2, $filteredDocs);

        $refs = array_map(function (array $doc) {
            return $doc['foo']['ref'];
        }, $filteredDocs);

        $this->assertEquals([$secondDocId, $thirdDocId], $refs);
    }

    /**
     * @test
     */
    public function it_handles_not_filter_nested_in_and_filter()
    {
        $collectionName = 'test_not_filter_nested_in_and_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['foo' => ['bar' => 'bas'], 'ref' => $firstDocId]);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['foo' => ['bar' => 'bat'], 'ref' => $secondDocId]);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['foo' => ['bar' => 'bat'], 'ref' => $thirdDocId]);

        $filteredDocs = \iterator_to_array($this->documentStore->filterDocs(
            $collectionName,
            new AndFilter(
                new EqFilter('foo.bar', 'bat'),
                new NotFilter(
                    new EqFilter('ref', $secondDocId)
                )
            )
        ));

        $this->assertCount(1, $filteredDocs);

        $refs = array_map(function (array $doc) {
            return $doc['ref'];
        }, $filteredDocs);

        $this->assertEquals([$thirdDocId], $refs);
    }

    /**
     * @test
     */
    public function it_retrieves_doc_ids_by_filter()
    {
        $collectionName = 'test_not_filter_nested_in_and_filter';
        $this->documentStore->addCollection($collectionName);

        $firstDocId = Uuid::uuid4()->toString();
        $secondDocId = Uuid::uuid4()->toString();
        $thirdDocId = Uuid::uuid4()->toString();

        $this->documentStore->addDoc($collectionName, $firstDocId, ['number' => 10]);
        $this->documentStore->addDoc($collectionName, $secondDocId, ['number' => 20]);
        $this->documentStore->addDoc($collectionName, $thirdDocId, ['number' => 30]);

        $result = $this->documentStore->filterDocIds($collectionName, new OrFilter(
            new GtFilter('number', 21),
            new LtFilter('number', 19)
        ));

        $this->assertEquals([$firstDocId, $thirdDocId], $result);
    }

    /**
     * @test
     */
    public function it_counts_any_of_filter()
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

        $count = $this->documentStore->countDocs(
            $collectionName,
            new AnyOfFilter("foo", ["bar", "bat"])
        );

        $this->assertSame(2, $count);
    }

    /**
     * @test
     */
    public function it_finds_partial_docs()
    {
        $collectionName = 'test_find_partial_docs';
        $this->documentStore->addCollection($collectionName);

        $docAId = Uuid::uuid4()->toString();
        $docA = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    'nested' => 42
                ]
            ],
            'baz' => 'bat',
        ];
        $this->documentStore->addDoc($collectionName, $docAId, $docA);

        $docBId = Uuid::uuid4()->toString();
        $docB = [
            'some' => [
                'prop' => 'bar',
                'other' => [
                    'nested' => 43
                ],
                //'baz' => 'bat', missing so should be null
            ],
        ];
        $this->documentStore->addDoc($collectionName, $docBId, $docB);

        $docCId = Uuid::uuid4()->toString();
        $docC = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    //'nested' => 42, missing, so should be null
                    'ignoredNested' => 'value'
                ]
            ],
            'baz' => 'bat',
        ];
        $this->documentStore->addDoc($collectionName, $docCId, $docC);

        $partialSelect = new PartialSelect([
            'some.alias' => 'some.prop', // Nested alias <- Nested field
            'magicNumber' => 'some.other.nested', // Top level alias <- Nested Field
            'baz', // Top level field,
        ]);

        $result = iterator_to_array($this->documentStore->findPartialDocs($collectionName, $partialSelect, new AnyFilter()));

        $this->assertEquals([
            'some' => [
                'alias' => 'foo',
            ],
            'magicNumber' => 42,
            'baz' => 'bat',
        ], $result[$docAId]);

        $this->assertEquals([
            'some' => [
                'alias' => 'bar',
            ],
            'magicNumber' => 43,
            'baz' => null,
        ], $result[$docBId]);

        $this->assertEquals([
            'some' => [
                'alias' => 'foo',
            ],
            'magicNumber' => null,
            'baz' => 'bat',
        ], $result[$docCId]);
    }

    /**
     * @test
     */
    public function it_applies_merge_alias_for_nested_fields_if_specified()
    {
        $collectionName = 'test_applies_merge_alias';
        $this->documentStore->addCollection($collectionName);

        $docAId = Uuid::uuid4()->toString();
        $docA = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    'nested' => 42
                ]
            ],
            'baz' => 'bat',
        ];
        $this->documentStore->addDoc($collectionName, $docAId, $docA);

        $docBId = Uuid::uuid4()->toString();
        $docB = [
            'differentTopLevel' => [
                'prop' => 'bar',
                'other' => [
                    'nested' => 43
                ],
            ],
            'baz' => 'bat',
        ];
        $this->documentStore->addDoc($collectionName, $docBId, $docB);

        $docCId = Uuid::uuid4()->toString();
        $docC = [
            'some' => [
                'prop' => 'foo',
                'other' => [
                    'nested' => 43
                ],
            ],
            //'baz' => 'bat', missing top level
        ];
        $this->documentStore->addDoc($collectionName, $docCId, $docC);

        $partialSelect = new PartialSelect([
            '$merge' => 'some', // $merge alias <- Nested field
            'baz', // Top level field
        ]);

        $result = iterator_to_array($this->documentStore->findPartialDocs($collectionName, $partialSelect, new AnyFilter()));

        $this->assertEquals([
            'prop' => 'foo',
            'other' => [
                'nested' => 42
            ],
            'baz' => 'bat'
        ], $result[$docAId]);

        $this->assertEquals([
            'baz' => 'bat',
        ], $result[$docBId]);

        $this->assertEquals([
            'prop' => 'foo',
            'other' => [
                'nested' => 43
            ],
            'baz' => null
        ], $result[$docCId]);
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
