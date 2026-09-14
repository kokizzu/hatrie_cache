#!/usr/bin/env bash
set -euo pipefail

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if ! git show :Makefile | rg -q '^\.PHONY: test-ch012-c203$'; then
  printf '%s\n' 'staged Makefile is missing the CH-012 test target' >&2
  exit 1
fi
