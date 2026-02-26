<?php

namespace App;

use Exception;



final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $map = [];
        $file = fopen($inputPath, 'r');
        $block = '';
        $lines_read = 0;
        for (;;) {
            $block .= fread($file, 0x10000);
            $blen = strlen($block) - 200; // 200 >= max line length, stop early to avoid starting to parse a line that straddles the boundary
            if ($blen <= 0) {
                $blen += 200; // we're on the last block, no early-stop needed
                if ($blen == 0) break; // eof
            };
            $idx = 0;
            while ($idx < $blen) {
                $comma = strpos($block, ',', $idx + 25);
                $path = substr($block, $idx + 25, ($comma - $idx) - 25);
                $date = ($block[$comma + 4]-1)*372 +
                        (substr($block, $comma + 6, 2)-1)*31 +
                        (substr($block, $comma + 9, 2)-1);

                //print($path.":".$date."\n");
                if (!isset($map[$path])) {
                    $map[$path] = array_fill(0, 2232, 0);
                }
                $map[$path][$date]++;
                //$foo = $path*2232+$date;
                //$array[$foo] = $array[$foo] + 1;

                //print($date."\n");
                //print($path."\n");
                //print($date."\n");
                $idx = $comma + 27;
                $lines_read++;
                //print($idx."\n");
            }
            $block = substr($block, $idx); // remainder
            //print($lines_read."\n");
        }

        print("read $lines_read lines\n");

        $datelut = [];
        for ($y = 1; $y <= 6; $y++) {
            for ($mm = 1; $mm <= 12; $mm++) {
                for ($dd = 1; $dd <= 31; $dd++) {
                    $datelut[] = sprintf("%d-%02d-%02d", $y, $mm, $dd);
                }
            }
        }

        print("done building date table\n");

        $outHandle = fopen($outputPath, 'wb');
        fwrite($outHandle, "{\n");
        foreach ($map as $slug => $dates) {
            $datefmt = [];
            foreach($dates as $dateid=>$count) {
                if ($count > 0) {
                    $datefmt[] = $datelut[$dateid].'": '.$count;
                }
            }
            $joined = implode(",\n        \"202", $datefmt);
            fwrite($outHandle, "    \"\\/blog\\/$slug\": {\n        \"202$joined\n    },\n");
        }
        fseek($outHandle, -2, SEEK_CUR); // unwind last comma+newline
        fwrite($outHandle, "\n}");
        fclose($outHandle);
    }
}
