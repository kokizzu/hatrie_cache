#!/bin/sh
set -eu
test ! -e scripts/inspect-chg02.sh
test ! -e scripts/inspect-chg02-benchmark.sh
test ! -e scripts/inspect-chg02-scripts.sh
test ! -e scripts/inspect-chg02-docs.sh
test ! -e scripts/inspect-chg02-docs-short.sh
test ! -e scripts/inspect-chg02-inspiration-tail.sh
test ! -e scripts/inspect-chg02-makefile.sh
git diff --check
git diff --cached --check
git status --short
git diff --stat
git diff --cached --stat
