#!/bin/sh
set -eu

git add BENCHMARK.md scripts/deliver-sql-index-generation-docs.sh Makefile
git diff --cached --check
git commit -m 'docs(sql): record index generation write tradeoff'
git push origin master
