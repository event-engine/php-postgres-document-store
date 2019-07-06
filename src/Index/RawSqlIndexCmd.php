<?php
declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres\Index;

use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\Postgres\Exception\InvalidArgumentException;

final class RawSqlIndexCmd implements Index
{
    /**
     * @var string|null
     */
    private $name;

    /**
     * @var string
     */
    private $sql;

    public static function fromArray(array $data): Index
    {
        if(!array_key_exists('sql', $data)) {
            throw new InvalidArgumentException("Data array misses raw sql stmt!");
        }

        return new self($data['sql'], $data['name'] ?? null);
    }

    public function __construct(string $sql, string $name = null)
    {
        $this->sql = $sql;
        $this->name = $name;
    }


    public function toArray()
    {
        return [
            'sql' => $this->sql,
            'name' => $this->name,
        ];
    }

    public function sql(): string
    {
        return $this->sql;
    }

    public function name(): ?string
    {
        return $this->name;
    }
}
