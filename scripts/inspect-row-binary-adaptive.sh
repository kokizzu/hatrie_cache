#!/usr/bin/env bash
set -euo pipefail
rg -n -A 90 'func (Encode|Decode)SQLRowBinaryAdaptive|BenchmarkSQLRowBinaryAdaptive|TestSQLRowBinaryAdaptive' hat/hatSql
