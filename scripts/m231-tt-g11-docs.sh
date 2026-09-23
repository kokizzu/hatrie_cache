#!/usr/bin/env bash
set -euo pipefail

test -s TTG11_WAL_SEGMENT_COMPRESSION.md
rg -n '^## TT-G11 WAL Segment Compression$' BENCHMARK.md
rg -n 'TTG11_WAL_SEGMENT_COMPRESSION.md' README.md
rg -n '^\| TT-G11 \|.*Implemented and measured' IDEA_GAP_CATALOG.md
