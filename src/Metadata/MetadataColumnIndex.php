<?php
declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres\Metadata;

use EventEngine\DocumentStore\Index;
use EventEngine\DocumentStore\Postgres\Exception\InvalidArgumentException;

final class MetadataColumnIndex implements Index
{
    /**
     * @var Column[]
     */
    private $columns;

    /**
     * @var Index
     */
    private $indexCmd;

    public static function fromArray(array $data): Index
    {
        if(!array_key_exists('column', $data)) {
            throw new InvalidArgumentException('Missing key columns in index data');
        }

        if(!array_key_exists('index', $data)) {
            throw new InvalidArgumentException('Missing key index in data');
        }

        if(!array_key_exists('indexClass', $data)) {
            throw new InvalidArgumentException('Missing key indexClass in data');
        }

        $indexClass = $data['indexClass'];
        $index = $indexClass::fromArray($data['index']);

        return new self($index, ...array_map(function (string $columnSql) {
            return new Column($columnSql);
        }, $data['columns']));
    }

    public function __construct(Index $indexCmd, Column ...$columns)
    {
        $this->columns = $columns;
        $this->indexCmd = $indexCmd;
    }

    /**
     * @return Column[]
     */
    public function columns(): array
    {
        return $this->columns;
    }

    /**
     * @return Index
     */
    public function indexCmd(): Index
    {
        return $this->indexCmd;
    }

    public function toArray()
    {
        return [
            'columns' => array_map(function (Column $column) {
                return $column->sql();
            }, $this->columns),
            'index' => $this->indexCmd->toArray(),
            'indexClass' => get_class($this->indexCmd),
        ];
    }
}
