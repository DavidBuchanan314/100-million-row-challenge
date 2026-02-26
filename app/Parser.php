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
    private const THREAD_COUNT = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        print("hello\n");

        $fileSize = filesize($inputPath);

        // Calculate split points for 8 threads
        $chunkSize = (int)($fileSize / self::THREAD_COUNT);
        $splitPoints = $this->findSplitPoints($inputPath, $fileSize, $chunkSize);

        print("splitting file into " . self::THREAD_COUNT . " segments\n");

        // Create temporary files for IPC
        $tmpFiles = [];
        $pids = [];

        // Fork child processes
        for ($i = 0; $i < self::THREAD_COUNT; $i++) {
            $start = $splitPoints[$i];
            $end = ($i < self::THREAD_COUNT - 1) ? $splitPoints[$i + 1] : $fileSize;
            $length = $end - $start;

            $tmpFile = tempnam(sys_get_temp_dir(), 'parser' . $i . '_');
            $tmpFiles[$i] = $tmpFile;

            $pid = pcntl_fork();

            if ($pid == -1) {
                throw new RuntimeException("Could not fork process");
            } elseif ($pid == 0) {
                // Child process - Process file segment
                $this->processFileSegment($inputPath, $start, $length, $tmpFile);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        // Parent process - wait for all children
        $allSuccess = true;
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
            if ($status !== 0) {
                $allSuccess = false;
            }
        }

        // Check if children exited successfully
        if (!$allSuccess) {
            foreach ($tmpFiles as $tmpFile) {
                if (file_exists($tmpFile)) {
                    unlink($tmpFile);
                }
            }
            throw new RuntimeException("Child processes failed");
        }

        print("merging results from " . self::THREAD_COUNT . " threads...\n");

        // Read results from all temporary files
        $allResults = [];
        foreach ($tmpFiles as $tmpFile) {
            $allResults[] = unserialize(file_get_contents($tmpFile));
            unlink($tmpFile);
        }

        // Merge all results
        $this->mergeResults($allResults, $outputPath);

        print("done writing\n");
    }

    private function findSplitPoints(string $inputPath, int $fileSize, int $chunkSize): array
    {
        $splitPoints = [0];
        $handle = fopen($inputPath, 'rb');

        if (!$handle) {
            throw new RuntimeException("Cannot open file: {$inputPath}");
        }

        for ($i = 1; $i < self::THREAD_COUNT; $i++) {
            $targetPos = $i * $chunkSize;
            fseek($handle, $targetPos);

            // Read until next newline to get a clean split
            while (!feof($handle) && fgetc($handle) !== "\n") {}
            $splitPoints[] = ftell($handle);
        }

        fclose($handle);
        return $splitPoints;
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

                $commaPos = strpos($line, ',', 25);

                $path = substr($line, 25, $commaPos - 25);
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
            $commaPos = strpos($remaining, ',', 25);
            $path = substr($remaining, 25, $commaPos - 25);
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

            $offset = $pathcIndices[$pathc] * self::TIMESTAMP_COUNT + $datecIndices[$datec];
            $matrix[$offset]++;
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

        print("thread completed, serializing\n");
        file_put_contents($outputFile, serialize($results));
    }

    private function mergeResults(array $allResults, string $outputPath): void
    {
        // Start with first result as base
        [$matrix, $pathIndices, $dateIndices, $nextPathIdx, $nextDateIdx] = $allResults[0];

        // Merge remaining results
        for ($i = 1; $i < count($allResults); $i++) {
            [$matrix2, $pathIndices2, $dateIndices2, $nextPathIdx2, $nextDateIdx2] = $allResults[$i];

            // Merge path indices
            foreach ($pathIndices2 as $path => $idx) {
                if (!isset($pathIndices[$path])) {
                    $pathIndices[$path] = $nextPathIdx++;
                }
            }

            // Merge date indices
            foreach ($dateIndices2 as $date => $idx) {
                if (!isset($dateIndices[$date])) {
                    $dateIndices[$date] = $nextDateIdx++;
                }
            }

            // Create remapping arrays
            $pathRemap = [];
            foreach ($pathIndices2 as $path => $oldIdx) {
                $pathRemap[$oldIdx] = $pathIndices[$path];
            }

            $dateRemap = [];
            foreach ($dateIndices2 as $date => $oldIdx) {
                $dateRemap[$oldIdx] = $dateIndices[$date];
            }

            // Merge matrix with remapping
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
                        $matrix[$baseOffsetNew + $newDateIdx] += $val;
                    }
                }
            }
        }

        print("done reading\n");

        // Sort dates
        $sortedDates = array_keys($dateIndices);
        sort($sortedDates, SORT_STRING);

        // Create path lookup with escaped slashes
        $pathLookup = array_keys($pathIndices);

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
                    $count = $matrix[$baseOffset + $dateIdx];

                    if ($count > 0) {
                        $dateEntries[] = '        "' . $date . '": ' . $count;
                    }
                }

                if (empty($dateEntries)) {
                    continue;
                }

                if ($firstPath) {
                    fwrite($outHandle, '    "\\/blog\\/' . $path . '": {' . "\n");
                    $firstPath = false;
                } else {
                    fwrite($outHandle, ",\n    \"\\/blog\\/" . $path . '": {' . "\n");
                }

                fwrite($outHandle, implode(",\n", $dateEntries) . "\n    }");
            }

            fwrite($outHandle, "\n}");
        } finally {
            fclose($outHandle);
        }
    }
}
