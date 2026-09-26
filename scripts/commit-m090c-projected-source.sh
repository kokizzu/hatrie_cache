#!/bin/sh
set -eu

git diff --cached --check
git commit -m "feat(sql): add projected materialized source pushdown"
