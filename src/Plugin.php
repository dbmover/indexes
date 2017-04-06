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

class Plugin extends Core\Plugin
{
    private $requestedIndexes = [];

    public function __invoke(string $sql) : string
    {
        if (preg_match_all(
            "@^CREATE\s+(UNIQUE\s+)?INDEX\s+([^\s]+?)?\s*ON\s+([^\s\(]+)\s*\((.*?)\);$@ms",
            $sql,
            $matches,
            PREG_SET_ORDER
        )) {
            foreach ($matches as $index) {
                $name = strlen($index[2])
                    ? $index[2]
                    : $index[3].'_'.preg_replace("@,\s*@", '_', $index[4]).'_idx';
                $index[4] = preg_replace("@,\s+@", ',', $index[4]);
                $this->requestedIndexes[$name] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^ALTER TABLE\s+([^\s]+?)\s+ADD PRIMARY KEY\((.*?)\)@", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $this->requestedIndex["{$index[1]}_PRIMARY"] = $index;
                $sql = str_replace($index[0], '', $sql);
            }
        }
        if (preg_match_all("@^CREATE TABLE\s+([^\s]+?)\s+.*?^\s*([^\s]+)\s.*?PRIMARY KEY@ms", $sql, $matches, PREG_SET_ORDER)) {
            foreach ($matches as $index) {
                $name = "{$index[1]}_PRIMARY";
                $this->requestedIndexes[$name] = [
                    "ALTER TABLE {$index[1]} ADD PRIMARY KEY({$index[2]})",
                    'UNIQUE',
                    $index[1],
                    '',
                    $index[2],
                ];
            }
        }

        // Check against existing indexes
        foreach ($this->existingIndexes() as $old) {
            if ($old['INDEX_NAME'] == 'PRIMARY') {
                $old['INDEX_NAME'] = "{$old['TABLE_NAME']}_PRIMARY";
            }
            if (!isset($this->requestedIndexes[$old['INDEX_NAME']])) {
                if (preg_match('@_PRIMARY$@', $old['INDEX_NAME'])) {
                    $this->loader->addOperation("ALTER TABLE {$old['TABLE_NAME']} DROP PRIMARY KEY;");
                } else {
                    $this->loader->addOperation("ALTER TABLE {$old['TABLE_NAME']} DROP INDEX {$old['INDEX_NAME']};");
                }
            } else {
                if ($old['COLUMN_NAME'] != $this->requestedIndexes[$old['INDEX_NAME']][4]
                    || $old['NON_UNIQUE'] != ($this->requestedIndexes[$old['INDEX_NAME']][1] == 'UNIQUE' ? 0 : 1)
                ) {
                    // Index has changed, so it needs to be rebuilt.
                    if (preg_match('@_PRIMARY$@', $old['INDEX_NAME'])) {
                        $this->loader->addOperation("ALTER TABLE {$old['TABLE_NAME']} DROP PRIMARY KEY;");
                    } else {
                        $this->loader->addOperation("ALTER TABLE {$old['TABLE_NAME']} DROP INDEX {$old['INDEX_NAME']};");
                    }
                } else {
                    // Index is already there, so ignore it.
                    unset($this->requestedIndexes[$old['INDEX_NAME']]);
                }
            }
        }

        return $sql;
    }

    public function __destruct()
    {
        foreach ($this->requestedIndexes as $index) {
            $this->loader->addOperation($index[0]);
        }
    }

    private function existingIndexes()
    {
        $stmt = $this->loader->getPdo()->prepare("SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ?");
        $stmt->execute([$this->loader->database]);
        $existing = [];
        while (false !== ($row = $stmt->fetch(PDO::FETCH_ASSOC))) {
            if (!isset($existing[$row['INDEX_NAME']])) {
                $existing[$row['INDEX_NAME']] = $row;
            } else {
                $existing[$row['INDEX_NAME']]['COLUMN_NAME'] .= ",{$row['COLUMN_NAME']}";
            }
        }
        return $existing;
    }
}

