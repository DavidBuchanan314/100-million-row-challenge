<?php

namespace App;

use Exception;
use SplFixedArray;
use RuntimeException;

final class Parser
{
    private const URL_COUNT = 268;
    private const TIMESTAMP_COUNT = 10000;
    
    public function parse(string $inputPath, string $outputPath): void
    {
        print("hello\n");
        // Pre-allocate matrix
        //$matrix = new SplFixedArray(self::URL_COUNT * self::TIMESTAMP_COUNT);
        //$matrix = SplFixedArray::fromArray(array_fill(0, count($matrix), 0), false);
        $matrix = array_fill(0, self::URL_COUNT * self::TIMESTAMP_COUNT, 0);
        print("array setup done\n");
        
        $pathIndices = [];
        $dateIndices = [];
        $nextPathIdx = 0;
        $nextDateIdx = 0;
        
        // Memory-map the file for fastest reading
        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            throw new RuntimeException("Cannot open file: {$inputPath}");
        }

        stream_set_chunk_size($handle, 1024 * 1024);

        try {
            $buffer = '';
            while (!feof($handle)) {
                $buffer .= fread($handle, 65536); // 64KB chunks
                $lines = explode("\n", $buffer);
                $buffer = array_pop($lines); // Last line might be incomplete

                foreach ($lines as $line) {
                    if ($line === '') continue;
                    
                    // Manual parsing without explode for speed
                    $commaPos = strpos($line, ',', 19);

                    $path = substr($line, 19, $commaPos - 19);
                    $date = substr($line, $commaPos + 1, 10);

                    // Use isset for fastest array key check
                    if (!isset($pathIndices[$path])) {
                        $pathIndices[$path] = $nextPathIdx++;
                    }
                    if (!isset($dateIndices[$date])) {
                        $dateIndices[$date] = $nextDateIdx++;
                    }
                    
                    $idx = $pathIndices[$path] * self::TIMESTAMP_COUNT + $dateIndices[$date];
                    $matrix[$idx] = $matrix[$idx] + 1;
                }
            }
            
            // Process final buffer
            foreach (explode("\n", $buffer) as $line) {
                if ($line === '') continue;
                
                // Manual parsing without explode for speed
                $commaPos = strpos($line, ',');
                if ($commaPos === false) continue;
                
                $path = substr($line, 19, $commaPos - 19);
                $date = substr($line, $commaPos + 1, 10);

                // Use isset for fastest array key check
                if (!isset($pathIndices[$path])) {
                    $pathIndices[$path] = $nextPathIdx++;
                }
                if (!isset($dateIndices[$date])) {
                    $dateIndices[$date] = $nextDateIdx++;
                }
                
                $idx = $pathIndices[$path] * self::TIMESTAMP_COUNT + $dateIndices[$date];
                $matrix[$idx] = $matrix[$idx] + 1;
            }
            
        } finally {
            fclose($handle);
        }

        print("done reading\n");

        // Create reverse mapping for dates to sort them
        $dateKeys = array_flip($dateIndices);
        $sortedDates = $dateKeys;
        sort($sortedDates);

        // Create reverse mapping for paths
        $pathKeys = array_flip($pathIndices);

        foreach ($pathKeys as $index => $path) {
            $pathKeys[$index] = str_replace('/', '\/', $path);
        }

        // Open output file for writing
        $outHandle = fopen($outputPath, 'wb');
        if (!$outHandle) {
            throw new RuntimeException("Cannot open output file: {$outputPath}");
        }

        try {
            // Write opening brace
            fwrite($outHandle, "{\n");
            
            $firstPath = true;
            
            // Iterate through all paths
            for ($pathIdx = 0; $pathIdx < $nextPathIdx; $pathIdx++) {
                $path = $pathKeys[$pathIdx];
                $pathData = [];
                
                // Check if path has any data by iterating through dates
                $dateEntries = [];
                
                // Build date entries for this path
                foreach ($sortedDates as $date) {
                    $dateIdx = $dateIndices[$date];
                    $idx = $pathIdx * self::TIMESTAMP_COUNT + $dateIdx;
                    $count = $matrix[$idx];
                    
                    if ($count > 0) {
                        $dateEntries[] = sprintf('        "%s": %d', $date, $count);
                    }
                }
             
                if ($firstPath) {
                    fwrite($outHandle, sprintf("    \"%s\": {\n%s\n    }", $path,  implode(",\n", $dateEntries)));
                } else {
                    fwrite($outHandle, sprintf(",\n    \"%s\": {\n%s\n    }", $path,  implode(",\n", $dateEntries)));
                }
                $firstPath = false;
            }
            
            // Write closing brace
            fwrite($outHandle, "\n}");
            
        } finally {
            fclose($outHandle);
        }

        print("done writing\n");
    }
}
