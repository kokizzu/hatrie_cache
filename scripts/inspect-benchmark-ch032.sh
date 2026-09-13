#!/bin/sh
set -eu

rg -n -C 4 'Enum|Decimal|RowBinary|CH-0' BENCHMARK.md
