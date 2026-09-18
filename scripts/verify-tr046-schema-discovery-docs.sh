#!/usr/bin/env bash
set -eu
test -s TR046_SCHEMA_DDL_DISCOVERY.md
grep -q 'TR-46' INSPIRATION_BACKLOG.md
grep -q 'TR-46' BENCHMARK.md
grep -q 'TR046_SCHEMA_DDL_DISCOVERY.md' README.md
