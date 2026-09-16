#!/bin/sh
set -eu
test -s CHG01_EXTERNAL_GROUP_SPILL.md
rg -n '^## CH-G01: Bounded External `GROUP BY` Aggregation Spill$' BENCHMARK.md
rg -n 'CHG01_EXTERNAL_GROUP_SPILL.md' README.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n 'C074 External aggregation' INSPIRATION.md
