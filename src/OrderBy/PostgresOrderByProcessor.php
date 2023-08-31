<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres\OrderBy;

use EventEngine\DocumentStore;
use EventEngine\DocumentStore\OrderBy\OrderBy;

final class PostgresOrderByProcessor implements OrderByProcessor
{
    /**
     * @var bool
     */
    private $useMetadataColumns;

    public function __construct(bool $useMetadataColumns = false)
    {
        $this->useMetadataColumns = $useMetadataColumns;
    }

    public function process(OrderBy $orderBy): OrderByClause
    {
        [$orderByClause, $args] = $this->processOrderBy($orderBy);

        return new OrderByClause($orderByClause, $args);
    }

    private function processOrderBy(OrderBy $orderBy): array
    {
        if($orderBy instanceof DocumentStore\OrderBy\AndOrder) {
            [$sortA, $sortAArgs] = $this->processOrderBy($orderBy->a());
            [$sortB, $sortBArgs] = $this->processOrderBy($orderBy->b());

            return ["$sortA, $sortB", array_merge($sortAArgs, $sortBArgs)];
        }

        if ($orderBy instanceof DocumentStore\OrderBy\DocId) {
            $direction = $orderBy->direction();

            return ["id $direction", []];
        }

        /** @var DocumentStore\OrderBy\Asc|DocumentStore\OrderBy\Desc $orderBy */
        $direction = $orderBy instanceof DocumentStore\OrderBy\Asc ? 'ASC' : 'DESC';
        $prop = $this->propToJsonPath($orderBy->prop());

        return ["{$prop} $direction", []];
    }

    private function propToJsonPath(string $field): string
    {
        if($this->useMetadataColumns && strpos($field, 'metadata.') === 0) {
            return str_replace('metadata.', '', $field);
        }

        return "doc->'" . str_replace('.', "'->'", $field) . "'";
    }
}
