<?php

/**
 * @package Dbmover
 * @subpackage Indexes
 *
 * Migrate all indexes.
 */

namespace Dbmover\Indexes;

use Dbmover\Core;
use PDO;

abstract class Plugin extends Core\Plugin
{
    const REGEX = "@^CREATE\s+(UNIQUE\s+)?INDEX\s+([^\s]+?)\s+ON\s+([^\s\(]+)(\s+USING \w+)?\s*\((.*?)\);$@ms";

    public function __invoke(string $sql) : string
    {
        $requestedIndexes = [];
        if (preg_match_all(static::REGEX, $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $name = strlen($index[2])
                    ? $index[2]
                    : $index[3].'_'.preg_replace("@,\s*@", '_', $index[5]).'_idx';
                $index[5] = preg_replace("@,\s+@", ',', $index[5]);
                $index[4] = trim($index[4]);
                $requestedIndexes[$name] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^ALTER TABLE\s+([^\s]+?)\s+ADD PRIMARY KEY\((.*?)\)@", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $index[5] = $index[4];
                $index[4] = '';
                $requestedIndexes["{$index[1]}_PRIMARY"] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^CREATE TABLE\s+([^\s]+?)\s+.*?^\s*([^\s]+)\s.*?PRIMARY KEY@ms", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $name = "{$index[1]}_PRIMARY";
                $requestedIndexes[$name] = [
                    "ALTER TABLE {$index[1]} ADD PRIMARY KEY({$index[2]})",
                    'UNIQUE',
                    $index[1],
                    '',
                    '',
                    $index[2],
                ];
            }
        }

        // Check against existing indexes
        foreach ($this->existingIndexes() as $old) {
            if ($old['index_name'] == 'PRIMARY') {
                $old['index_name'] = "{$old['table_name']}_PRIMARY";
            }
            if (!isset($requestedIndexes[$old['index_name']])
                || $old['column_name'] != $requestedIndexes[$old['index_name']][5]
                || $old['non_unique'] != ($requestedIndexes[$old['index_name']][1] == 'UNIQUE' ? 0 : 1)
                || $old['type'] != $requestedIndexes[$old['index_name']][4]
            ) {
                var_dump($old, $requestedIndexes[$old['index_name']]);
                // Index has changed, so it needs to be rebuilt.
                if (preg_match('@_PRIMARY$@', $old['index_name'])) {
                    $this->loader->addOperation($this->dropPrimaryKey($old['table_name']));
                } else {
                    $this->loader->addOperation($this->dropIndex($old['index_name'], $old['table_name']));
                }
            } else {
                // Index is already there, so ignore it.
                unset($requestedIndexes[$old['index_name']]);
            }
        }
        foreach ($requestedIndexes as $index) {
            $this->defer($index[0]);
        }
        return $sql;
    }

    protected abstract function existingIndexes() : array;
}

