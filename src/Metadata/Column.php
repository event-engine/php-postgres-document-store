<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

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
