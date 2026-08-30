#!/usr/bin/env sh
set -eu

git add -- INDEX_PROPOSAL.md scripts/deliver-index-proposal-status.sh Makefile
git diff --cached --check
git commit -m 'docs(sql): record streamed group aggregation status'
git push origin master
