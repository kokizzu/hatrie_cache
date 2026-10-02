#!/usr/bin/env bash
set -euo pipefail

test -f CHU47_SQL_DICTIONARY_FUNCTIONS.md
rg -q 'CHU47_SQL_DICTIONARY_FUNCTIONS.md' README.md
rg -q 'ch-u47-sql-dictionary-functions' README.md
rg -q '## CH-U47 SQL Dictionary Functions' BENCHMARK.md
rg -q 'CH-U47.*Dictionary lookup functions.*Implemented' PRODUCT_IDEA_GAPS.md
