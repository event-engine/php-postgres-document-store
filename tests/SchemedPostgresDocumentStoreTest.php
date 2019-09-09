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

class SchemedPostgresDocumentStoreTest extends TestCase
{
    private CONST TABLE_PREFIX = 'test_';
    private CONST SCHEMA = 'test.';

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
    public function it_adds_collection_with_schema(): void
    {
        $this->documentStore->addCollection(self::SCHEMA . 'test');
        $this->assertFalse($this->documentStore->hasCollection('test'));
        $this->assertTrue($this->documentStore->hasCollection(self::SCHEMA . 'test'));
    }
}
