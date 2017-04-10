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
    const REGEX = "@^CREATE\s+(UNIQUE\s+)?INDEX\s+([^\s]+?)?\s*ON\s+([^\s\(]+)(\s+USING \w+)?\s*\((.*)\).*?;$@m";

    public $description = 'Checking index (re)creation...';

    public function __invoke(string $sql) : string
    {
        $requestedIndexes = [];
        if (preg_match_all(static::REGEX, $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $name = strlen($index[2])
                    ? $index[2]
                    : preg_replace("@[\W_]+@", '_', "{$index[3]}_{$index[5]}_idx");
                $index[5] = preg_replace("@,\s+@", ',', $index[5]);
                $index[1] = trim($index[1]);
                $index[4] = trim($index[4]);
                $requestedIndexes[$name] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^ALTER TABLE\s+([^\s]+?)\s+ADD PRIMARY KEY\((.*?)\)@", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $index[5] = $index[4];
                $name = "{$index[1]}_PRIMARY";
                $index[4] = $name;
                $requestedIndexes[$name] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^CREATE TABLE\s+([^\s]+?)\s+\((.*?)^\)@ms", $sql, $pktables, PREG_SET_ORDER)) {
            foreach ($pktables as $pktable) {
                if (preg_match("@^\s+([^\s]+).*?PRIMARY KEY@m", $pktable[0], $pk)
                    || preg_match("@^\s+PRIMARY KEY\((.*?)\)@m", $pktable[0], $pk)
                ) {
                    $name = "{$pktable[1]}_PRIMARY";
                    $requestedIndexes[$name] = [
                        "ALTER TABLE {$pktable[1]} ADD PRIMARY KEY({$pk[1]})",
                        'UNIQUE',
                        '',
                        '',
                        $name,
                        $pk[1]
                    ];
                }
            }
        }

        // Check against existing indexes
        foreach ($this->existingIndexes() as $old) {
            if ($old['index_name'] == 'PRIMARY') {
                $old['index_name'] = "{$old['table_name']}_PRIMARY";
            }
            if (!isset($requestedIndexes[$old['index_name']])
                || strtolower($old['column_name']) != strtolower($requestedIndexes[$old['index_name']][5])
                || $old['non_unique'] != ($requestedIndexes[$old['index_name']][1] == 'UNIQUE' ? 0 : 1)
                || (!preg_match("@_PRIMARY$@", $old['index_name'])
                    && strtolower($old['type']) != strtolower($requestedIndexes[$old['index_name']][4])
                )
            ) {
                // Index has changed, so it needs to be rebuilt.
                if (preg_match('@_PRIMARY$@', $old['index_name'])) {
                    $this->defer($this->dropPrimaryKey($old['index_name'], $old['table_name']));
                } else {
                    $this->defer($this->dropIndex($old['index_name'], $old['table_name']));
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

