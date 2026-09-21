#!/usr/bin/env bash
set -euo pipefail

git diff --check
git diff --cached --check
test ! -e scripts/inspect-mz026.sh
test ! -e scripts/inspect-mz026-tail.sh
test ! -e scripts/inspect-mz026-dictionary.sh
test ! -e scripts/inspect-mz026-probe.sh
test ! -e scripts/inspect-mz026-bench.sh
test ! -e scripts/print-mz026-test.sh
