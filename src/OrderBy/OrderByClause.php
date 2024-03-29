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

final class OrderByClause
{
    private $clause;
    private $args;

    public function __construct(?string $clause, array $args = [])
    {
        $this->clause = $clause;
        $this->args = $args;
    }

    public function clause(): ?string
    {
        return $this->clause;
    }

    public function args(): array
    {
        return $this->args;
    }
}
