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

use PDO;

final class TestUtil
{
    /**
     * @var PDO
     */
    private static $connection;

    public static function getConnection(): PDO
    {
        if (self::$connection === null) {
            $connectionParams = self::getConnectionParams();
            $separator = ' ';
            $dsn = 'pgsql:';
            $dsn .= 'host=' . $connectionParams['host'] . $separator;
            $dsn .= 'port=' . $connectionParams['port'] . $separator;
            $dsn .= 'dbname=' . $connectionParams['dbname'] . $separator;
            $dsn .= self::getCharsetValue($connectionParams['charset']) . $separator;
            $dsn = rtrim($dsn);

            $retries = 10; // keep trying for 10 seconds, should be enough
            while (null === self::$connection && $retries > 0) {
                try {
                    self::$connection = new PDO(
                        $dsn, $connectionParams['user'], $connectionParams['password'], $connectionParams['options']
                    );
                } catch (\PDOException $e) {
                    if (2002 !== $e->getCode()) {
                        throw $e;
                    }

                    $retries--;
                    sleep(1);
                }
            }
        }

        if (!self::$connection) {
            print "db connection could not be established. aborting...\n";
            exit(1);
        }

        try {
            self::$connection->rollBack();
        } catch (\PDOException $e) {
            // ignore
        }

        return self::$connection;
    }

    public static function getDatabaseName(): string
    {
        if (!self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return getenv('DB_NAME');
    }

    public static function getConnectionParams(): array
    {
        if (!self::hasRequiredConnectionParams()) {
            throw new \RuntimeException('No connection params given');
        }

        return self::getSpecifiedConnectionParams();
    }

    public static function tearDownDatabase(): void
    {
        $connection = self::getConnection();
        $statement = $connection->prepare('SELECT table_name FROM information_schema.tables WHERE table_schema = \'public\';');
        $connection->exec('DROP SCHEMA IF EXISTS prooph CASCADE');

        $statement->execute();
        $tables = $statement->fetchAll(PDO::FETCH_COLUMN);

        foreach ($tables as $table) {
            $connection->exec(sprintf('DROP TABLE "%s";', $table));
        }
    }

    public static function subMilliseconds(\DateTimeImmutable $time, int $ms): \DateTimeImmutable
    {
        //Create a 0 interval
        $interval = new \DateInterval('PT0S');
        //and manually add split seconds
        $interval->f = $ms / 1000;

        return $time->sub($interval);
    }

    private static function hasRequiredConnectionParams(): bool
    {
        $env = getenv();

        return isset(
            $env['DB_USERNAME'],
            $env['DB_HOST'],
            $env['DB_NAME'],
            $env['DB_PORT'],
            $env['DB_CHARSET']
        );
    }

    private static function getSpecifiedConnectionParams(): array
    {
        return [
            'user' => getenv('DB_USERNAME'),
            'password' => false !== getenv('DB_PASSWORD') ? getenv('DB_PASSWORD') : '',
            'host' => getenv('DB_HOST'),
            'dbname' => getenv('DB_NAME'),
            'port' => getenv('DB_PORT'),
            'charset' => getenv('DB_CHARSET'),
            'options' => [PDO::ATTR_ERRMODE => (int)getenv('DB_ATTR_ERRMODE')],
        ];
    }

    private static function getCharsetValue(string $charset): string
    {
        return "options='--client_encoding=$charset'";
    }
}
