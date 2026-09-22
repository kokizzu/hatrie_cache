#!/usr/bin/env bash
set -euo pipefail

test -f M248_MAINTAINED_RESULT_CACHE.md
rg -n 'M248_MAINTAINED_RESULT_CACHE|M248' README.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md
rg -n '^test-m248:|^benchmark-m248:|^benchmark-m248-serialized:|^race-m248:|^vet-m248:' Makefile
