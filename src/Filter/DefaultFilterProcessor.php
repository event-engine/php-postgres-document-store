<?php
/**
 * This file is part of the event-engine/php-postgres-document-store.
 * (c) 2019-2021 prooph software GmbH <contact@prooph.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace EventEngine\DocumentStore\Postgres\Filter;

use EventEngine\DocumentStore;
use EventEngine\DocumentStore\Filter\Filter;
use EventEngine\DocumentStore\Postgres\Exception\InvalidArgumentException;
use EventEngine\DocumentStore\Postgres\Exception\RuntimeException;

/**
 * Default filter processor class for converting a filter to a where clause.
 */
final class DefaultFilterProcessor implements FilterProcessor
{
    /**
     * @var bool
     */
    private $useMetadataColumns;

    public function __construct(bool $useMetadataColumns = false)
    {
        $this->useMetadataColumns = $useMetadataColumns;
    }

    /**
     * @param Filter $filter
     * @param int $argsCount
     * @return array
     */
    public function process(Filter $filter, $argsCount = 0): array
    {
        if($filter instanceof DocumentStore\Filter\AnyFilter) {
            if($argsCount > 0) {
                throw new InvalidArgumentException('AnyFilter cannot be used together with other filters.');
            }
            return [null, [], $argsCount];
        }

        if($filter instanceof DocumentStore\Filter\AndFilter) {
            [$filterA, $argsA, $argsCount] = $this->process($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->process($filter->bFilter(), $argsCount);
            return ["($filterA AND $filterB)", array_merge($argsA, $argsB), $argsCount];
        }

        if($filter instanceof DocumentStore\Filter\OrFilter) {
            [$filterA, $argsA, $argsCount] = $this->process($filter->aFilter(), $argsCount);
            [$filterB, $argsB, $argsCount] = $this->process($filter->bFilter(), $argsCount);
            return ["($filterA OR $filterB)", array_merge($argsA, $argsB), $argsCount];
        }

        switch (get_class($filter)) {
            case DocumentStore\Filter\DocIdFilter::class:
                /** @var DocumentStore\Filter\DocIdFilter $filter */
                return ["id = :a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\AnyOfDocIdFilter::class:
                /** @var DocumentStore\Filter\AnyOfDocIdFilter $filter */
                return $this->makeInClause('id', $filter->valList(), $argsCount);
            case DocumentStore\Filter\AnyOfFilter::class:
                /** @var DocumentStore\Filter\AnyOfFilter $filter */
                return $this->makeInClause($this->propToJsonPath($filter->prop()), $filter->valList(), $argsCount, $this->shouldJsonEncodeVal($filter->prop()));
            case DocumentStore\Filter\EqFilter::class:
                /** @var DocumentStore\Filter\EqFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop = :a$argsCount", ["a$argsCount" => $this->prepareVal($filter->val(), $filter->prop())], ++$argsCount];
            case DocumentStore\Filter\GtFilter::class:
                /** @var DocumentStore\Filter\GtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop > :a$argsCount", ["a$argsCount" => $this->prepareVal($filter->val(), $filter->prop())], ++$argsCount];
            case DocumentStore\Filter\GteFilter::class:
                /** @var DocumentStore\Filter\GteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop >= :a$argsCount", ["a$argsCount" => $this->prepareVal($filter->val(), $filter->prop())], ++$argsCount];
            case DocumentStore\Filter\LtFilter::class:
                /** @var DocumentStore\Filter\LtFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop < :a$argsCount", ["a$argsCount" => $this->prepareVal($filter->val(), $filter->prop())], ++$argsCount];
            case DocumentStore\Filter\LteFilter::class:
                /** @var DocumentStore\Filter\LteFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop <= :a$argsCount", ["a$argsCount" => $this->prepareVal($filter->val(), $filter->prop())], ++$argsCount];
            case DocumentStore\Filter\LikeFilter::class:
                /** @var DocumentStore\Filter\LikeFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                $propParts = explode('->', $prop);
                $lastProp = array_pop($propParts);
                $prop = implode('->', $propParts) . '->>'.$lastProp;
                return ["$prop iLIKE :a$argsCount", ["a$argsCount" => $filter->val()], ++$argsCount];
            case DocumentStore\Filter\NotFilter::class:
                /** @var DocumentStore\Filter\NotFilter $filter */
                $innerFilter = $filter->innerFilter();

                if (!$this->isPropFilter($innerFilter)) {
                    throw new RuntimeException('Not filter cannot be combined with a non prop filter!');
                }

                [$innerFilterStr, $args, $argsCount] = $this->process($innerFilter, $argsCount);

                if($innerFilter instanceof DocumentStore\Filter\AnyOfFilter || $innerFilter instanceof DocumentStore\Filter\AnyOfDocIdFilter) {
                    if ($argsCount === 0) {
                        return [
                            str_replace(' 1 != 1 ', ' 1 = 1 ', $innerFilterStr),
                            $args,
                            $argsCount
                        ];
                    }

                    $inPos = strpos($innerFilterStr, ' IN(');
                    $filterStr = substr_replace($innerFilterStr, ' NOT IN(', $inPos, 4 /* " IN(" */);
                    return [$filterStr, $args, $argsCount];
                }

                return ["NOT $innerFilterStr", $args, $argsCount];
            case DocumentStore\Filter\InArrayFilter::class:
                /** @var DocumentStore\Filter\InArrayFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                return ["$prop @> :a$argsCount", ["a$argsCount" => '[' . $this->prepareVal($filter->val(), $filter->prop()) . ']'], ++$argsCount];
            case DocumentStore\Filter\ExistsFilter::class:
                /** @var DocumentStore\Filter\ExistsFilter $filter */
                $prop = $this->propToJsonPath($filter->prop());
                $propParts = explode('->', $prop);
                $lastProp = trim(array_pop($propParts), "'");
                $parentProps = implode('->', $propParts);
                return ["JSONB_EXISTS($parentProps, '$lastProp')", [], $argsCount];
            default:
                throw new RuntimeException('Unsupported filter type. Got ' . get_class($filter));
        }
    }

    private function makeInClause(string $prop, array $valList, int $argsCount, bool $jsonEncode = false): array
    {
        if ($valList === []) {
            return [' 1 != 1 ', [], 0];
        }
        $argList = [];
        $params = \implode(",", \array_map(function ($val) use (&$argsCount, &$argList, $jsonEncode) {
            $param = ":a$argsCount";
            $argList["a$argsCount"] = $jsonEncode? \json_encode($val) : $val;
            $argsCount++;
            return $param;
        }, $valList));

        return ["$prop IN($params)", $argList, $argsCount];
    }

    private function shouldJsonEncodeVal(string $prop): bool
    {
        if($this->useMetadataColumns && strpos($prop, 'metadata.') === 0) {
            return false;
        }

        return true;
    }

    private function propToJsonPath(string $field): string
    {
        if($this->useMetadataColumns && strpos($field, 'metadata.') === 0) {
            return str_replace('metadata.', '', $field);
        }

        return "doc->'" . str_replace('.', "'->'", $field) . "'";
    }

    private function isPropFilter(Filter $filter): bool
    {
        switch (get_class($filter)) {
            case DocumentStore\Filter\AndFilter::class:
            case DocumentStore\Filter\OrFilter::class:
            case DocumentStore\Filter\NotFilter::class:
                return false;
            default:
                return true;
        }
    }

    private function prepareVal($value, string $prop)
    {
        if(!$this->shouldJsonEncodeVal($prop)) {
            return $value;
        }

        return \json_encode($value);
    }
}
