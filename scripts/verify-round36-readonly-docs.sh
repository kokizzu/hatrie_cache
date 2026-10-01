#!/usr/bin/env bash
set -euo pipefail

test -s TU06_REPLICA_READ_ONLY_GATE.md
rg -q 'TU06_REPLICA_READ_ONLY_GATE.md' README.md
rg -q '0.481 ns/op' BENCHMARK.md
rg -q 'ReadOnlyGate' PRODUCT_IDEA_GAPS.md
