#!/bin/sh
set -eu

make verify-benchmark-md-update
git diff --check -- BENCHMARK.md Makefile scripts/deliver-sql-columnar-string-bloom-segment-benchmark-docs.sh
git add BENCHMARK.md Makefile scripts/deliver-sql-columnar-string-bloom-segment-benchmark-docs.sh
git commit -m "docs(benchmark): refresh string segment results"
git push
