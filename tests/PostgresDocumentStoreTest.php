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

use PHPUnit\Framework\TestCase;
use EventEngine\DocumentStore\FieldIndex;
use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\MultiFieldIndex;
use EventEngine\DocumentStore\Postgres\PostgresDocumentStore;

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

    private function getIndexes(string $collectionName): array
    {
        $stmt = $this->connection->prepare(
            "select * from pg_indexes where schemaname = 'public' and tablename = :name"
        );
        $stmt->execute(['name' => self::TABLE_PREFIX . $collectionName]);
        return $stmt->fetchAll();
    }

}
