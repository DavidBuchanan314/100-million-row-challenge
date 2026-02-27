<?php

namespace App;

use Exception;



final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $datelut = [];
        for ($y = 1; $y <= 6; $y++) {
            for ($mm = 1; $mm <= 12; $mm++) {
                for ($dd = 1; $dd <= 31; $dd++) {
                    $datelut[] = sprintf("%d-%02d-%02d", $y, $mm, $dd);
                }
            }
        }
        $datelutlut = array_flip($datelut);

        print("done building date table\n");

        $map = [];
        $file = fopen($inputPath, 'r');
        $lines_read = 0;
        for (;;) {
            $block = fread($file, 0x10000).fgets($file);
            $blen = strlen($block);
            if ($blen == 0) break; // eof
            $idx = 0;
            while ($idx < $blen) {
                $comma = strpos($block, ',', $idx + 25);
                $path = substr($block, $idx + 25, ($comma - $idx) - 25);
                //$date = ($block[$comma + 4]-1)*372 +
                //        (substr($block, $comma + 6, 2)-1)*31 +
                //        (substr($block, $comma + 9, 2)-1);
                $date = $datelutlut[substr($block, $comma + 4, 7)];

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
            //print($lines_read."\n");
        }

        print("read $lines_read lines\n");
        return;

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
