<?php

namespace App;

use Exception;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $file = new \SplFileObject($inputPath, 'r');
        //$file->setFlags(\SplFileObject::READ_AHEAD);
        $block = '';
        $lines_read = 0;
        for (;;) {
            $block .= $file->fread(0x10000);
            $blen = strlen($block) - 200; // 200 >= max line length, stop early to avoid starting to parse a line that straddles the boundary
            if ($blen <= 0) {
                $blen += 200; // we're on the last block, no early-stop needed
                if ($blen == 0) break; // eof
            };
            $idx = 0;
            while ($idx < $blen) {
                $comma = strpos($block, ',', $idx + 25);
                $path = substr($block, $idx + 25, ($comma - $idx) - 25);
                $date = substr($block, $comma + 1, 10);
                //print($path."\n");
                //print($date."\n");
                $idx = $comma + 27;
                $lines_read++;
                //print($idx."\n");
            }
            $block = substr($block, $idx); // remainder
            //print($lines_read."\n");
        }

        print($lines_read."\n");
    }
}
