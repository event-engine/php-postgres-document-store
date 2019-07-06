<?php
declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres\Metadata;

final class Column
{
    private $sql;

    public function __construct(string $sql)
    {
        $this->sql = $sql;
    }

    public function sql(): string
    {
        return $this->sql;
    }
}
