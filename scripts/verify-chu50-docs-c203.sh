#!/bin/sh
set -eu

test -s CHU50_PART_WAL_CONSISTENCY.md
rg -F 'CHU50_PART_WAL_CONSISTENCY.md' README.md
rg -F '## CH-U50 Part/WAL-Consistent Backup Manifest' BENCHMARK.md
rg -F 'CH-U50' PRODUCT_IDEA_GAPS.md
rg -F 'Part/WAL-consistent backup manifest' ADOPTED_QUERY_ENGINE_IDEAS.md
