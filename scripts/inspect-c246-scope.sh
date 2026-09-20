#!/usr/bin/env bash
set -euo pipefail

rg -n -C 2 'C246|recompress|recompression|TTL-driven|TTL.*compress' \
  INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md
rg -n -C 2 'Recompress|recompress|Recompression|CompactionPolicy|compaction policy|LevelDBCompaction|TupleCompression|CompressionPolicy' \
  hat/hatDataStructure hat/hatCache --glob '*.go'
rg -n -C 3 'Adaptive.*Gorilla|C247|Tuple compression|TTL-driven' \
  ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md
rg -n 'recompress|recompression|compaction policy|CompactionPolicy|LevelDBCompaction|TupleCompression|C246|c246' \
  Makefile scripts --glob '*.sh' --glob 'Makefile'
