#!/bin/sh
set -eu

rg -n -C 3 'SQLIndexInt64|typed JSON|typed index|composite' README.md BENCHMARK.md INDEX_PROPOSAL.md
sed -n '1,90p' README.md
sed -n '13420,13510p' BENCHMARK.md
