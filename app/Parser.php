<?php

declare(strict_types=1);

namespace App;

use RuntimeException;

final class Parser
{
    private const URL_COUNT = 268;
    private const TIMESTAMP_COUNT = 10000;
    private const CHUNK_SIZE = 1024 * 64; // 64k chunks
    private const MATRIX_SIZE = self::URL_COUNT * self::TIMESTAMP_COUNT;

    public function parse(string $inputPath, string $outputPath): void
    {
        print("hello\n");

        $fileSize = filesize($inputPath);
        $midPoint = (int)($fileSize / 2);

        // Find line boundaries near the midpoint
        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            throw new RuntimeException("Cannot open file: {$inputPath}");
        }

        fseek($handle, $midPoint);

        // Read until next newline to get a clean split
        while (!feof($handle) && fgetc($handle) !== "\n") {}
        $splitPos = ftell($handle);
        fclose($handle);

        print("splitting file at position: {$splitPos}\n");

        // Create temporary files for IPC
        $tmpFile1 = tempnam(sys_get_temp_dir(), 'parser1_');
        $tmpFile2 = tempnam(sys_get_temp_dir(), 'parser2_');

        $pid1 = pcntl_fork();

        if ($pid1 == -1) {
            throw new RuntimeException("Could not fork process");
        } elseif ($pid1 == 0) {
            // Child process 1 - Process first half (0 to splitPos)
            $this->processFileSegment($inputPath, 0, $splitPos, $tmpFile1);
            exit(0);
        }

        $pid2 = pcntl_fork();

        if ($pid2 == -1) {
            throw new RuntimeException("Could not fork process");
        } elseif ($pid2 == 0) {
            // Child process 2 - Process second half (splitPos to end)
            $this->processFileSegment($inputPath, $splitPos, $fileSize - $splitPos, $tmpFile2);
            exit(0);
        }

        // Parent process - wait for both children
        pcntl_waitpid($pid1, $status1);
        pcntl_waitpid($pid2, $status2);

        // Check if children exited successfully
        if ($status1 !== 0 || $status2 !== 0) {
            unlink($tmpFile1);
            unlink($tmpFile2);
            throw new RuntimeException("Child processes failed");
        }

        print("merging results...\n");

        // Read results from temporary files
        $results1 = unserialize(file_get_contents($tmpFile1));
        $results2 = unserialize(file_get_contents($tmpFile2));

        unlink($tmpFile1);
        unlink($tmpFile2);

        // Merge the results
        [$matrix1, $pathIndices1, $dateIndices1, $nextPathIdx1, $nextDateIdx1] = $results1;
        [$matrix2, $pathIndices2, $dateIndices2, $nextPathIdx2, $nextDateIdx2] = $results2;

        // Merge path indices
        $pathIndices = $pathIndices1;
        $nextPathIdx = $nextPathIdx1;

        foreach ($pathIndices2 as $path => $idx) {
            if (!isset($pathIndices[$path])) {
                $pathIndices[$path] = $nextPathIdx++;
            }
        }

        // Merge date indices
        $dateIndices = $dateIndices1;
        $nextDateIdx = $nextDateIdx1;

        foreach ($dateIndices2 as $date => $idx) {
            if (!isset($dateIndices[$date])) {
                $dateIndices[$date] = $nextDateIdx++;
            }
        }

        // Merge second matrix with index remapping
        $pathRemap = [];
        foreach ($pathIndices2 as $path => $oldIdx) {
            $pathRemap[$oldIdx] = $pathIndices[$path];
        }

        $dateRemap = [];
        foreach ($dateIndices2 as $date => $oldIdx) {
            $dateRemap[$oldIdx] = $dateIndices[$date];
        }

        for ($oldPathIdx = 0; $oldPathIdx < $nextPathIdx2; $oldPathIdx++) {
            $newPathIdx = $pathRemap[$oldPathIdx] ?? null;
            if ($newPathIdx === null) continue;

            $baseOffsetOld = $oldPathIdx * self::TIMESTAMP_COUNT;
            $baseOffsetNew = $newPathIdx * self::TIMESTAMP_COUNT;

            for ($oldDateIdx = 0; $oldDateIdx < $nextDateIdx2; $oldDateIdx++) {
                $newDateIdx = $dateRemap[$oldDateIdx] ?? null;
                if ($newDateIdx === null) continue;

                $val = $matrix2[$baseOffsetOld + $oldDateIdx];
                if ($val > 0) {
                    $matrix1[$baseOffsetNew + $newDateIdx] += $val;
                }
            }
        }

        print("done reading\n");

        // Sort dates
        $sortedDates = array_keys($dateIndices);
        sort($sortedDates, SORT_STRING);

        // Create path lookup with escaped slashes
        $pathLookup = [];
        foreach ($pathIndices as $path => $idx) {
            $pathLookup[$idx] = str_replace('/', '\/', $path);
        }

        // Write output
        $outHandle = fopen($outputPath, 'wb');
        if (!$outHandle) {
            throw new RuntimeException("Cannot open output file: {$outputPath}");
        }

        try {
            fwrite($outHandle, "{\n");

            $firstPath = true;

            for ($pathIdx = 0; $pathIdx < $nextPathIdx; $pathIdx++) {
                $path = $pathLookup[$pathIdx] ?? null;
                if ($path === null) continue;

                $dateEntries = [];
                $baseOffset = $pathIdx * self::TIMESTAMP_COUNT;

                foreach ($sortedDates as $date) {
                    $dateIdx = $dateIndices[$date];
                    $count = $matrix1[$baseOffset + $dateIdx];

                    if ($count > 0) {
                        $dateEntries[] = '        "' . $date . '": ' . $count;
                    }
                }

                if (empty($dateEntries)) {
                    continue;
                }

                if ($firstPath) {
                    fwrite($outHandle, '    "' . $path . '": {' . "\n");
                    $firstPath = false;
                } else {
                    fwrite($outHandle, ",\n    \"" . $path . '": {' . "\n");
                }

                fwrite($outHandle, implode(",\n", $dateEntries) . "\n    }");
            }

            fwrite($outHandle, "\n}");

        } finally {
            fclose($outHandle);
        }

        print("done writing\n");
    }

    private function processFileSegment(string $inputPath, int $start, int $length, string $outputFile): void
    {
        $matrix = array_fill(0, self::MATRIX_SIZE, 0);

        $pathIndices = [];
        $dateIndices = [];
        $pathcIndices = [];
        $datecIndices = [];
        $nextPathIdx = 0;
        $nextDateIdx = 0;

        $handle = fopen($inputPath, 'rb');
        if (!$handle) {
            exit(1);
        }

        fseek($handle, $start);
        $bytesRead = 0;
        $buffer = '';
        $remaining = '';

        while ($bytesRead < $length) {
            $chunkSize = min(self::CHUNK_SIZE, $length - $bytesRead);
            $chunk = fread($handle, $chunkSize);
            //if ($chunk === false) break;

            $bytesRead += $chunkSize;
            $buffer = $remaining . $chunk;
            $lastNewline = strrpos($buffer, "\n");

            if ($lastNewline !== false) {
                $lines = explode("\n", substr($buffer, 0, $lastNewline));
                $remaining = substr($buffer, $lastNewline + 1);
            } else {
                $lines = [$buffer];
                $remaining = '';
            }

            foreach ($lines as $line) {
                if ($line === '') continue;

                $commaPos = strpos($line, ',', 19);
                if ($commaPos === false) continue;

                $path = substr($line, 19, $commaPos - 19);
                $pathc = crc32($path);
                $date = substr($line, $commaPos + 1, 10);
                $datec = crc32($date);

                if (!isset($pathcIndices[$pathc])) {
                    $pathIndices[$path] = $nextPathIdx;
                    $pathcIndices[$pathc] = $nextPathIdx++;
                }

                if (!isset($datecIndices[$datec])) {
                    $dateIndices[$date] = $nextDateIdx;
                    $datecIndices[$datec] = $nextDateIdx++;
                }

                $offset = $pathcIndices[$pathc] * self::TIMESTAMP_COUNT + $datecIndices[$datec];
                $matrix[$offset]++;
            }
        }

        // Process remaining data
        if ($remaining !== '') {
            $commaPos = strpos($remaining, ',', 19);
            if ($commaPos !== false) {
                $path = substr($remaining, 19, $commaPos - 19);
                $pathc = crc32($path);
                $date = substr($remaining, $commaPos + 1, 10);
                $datec = crc32($date);

                if (!isset($pathcIndices[$pathc])) {
                    $pathIndices[$path] = $nextPathIdx;
                    $pathcIndices[$pathc] = $nextPathIdx++;
                }

                if (!isset($datecIndices[$datec])) {
                    $dateIndices[$date] = $nextDateIdx;
                    $datecIndices[$datec] = $nextDateIdx++;
                }

                $$offset = $pathcIndices[$pathc] * self::TIMESTAMP_COUNT + $datecIndices[$datec];
                $matrix[$offset]++;
            }
        }

        fclose($handle);

        // Store results in temporary file
        $results = [
            $matrix,
            $pathIndices,
            $dateIndices,
            $nextPathIdx,
            $nextDateIdx
        ];

        print("serializing\n");
        file_put_contents($outputFile, serialize($results));
    }
}
