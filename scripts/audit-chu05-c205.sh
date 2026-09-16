#!/bin/sh
set -eu

git diff --check
git diff --stat
git status --short
rg -n -A 2 '^((inspect|prepare|cleanup)-chu05|format-chu05|test-chu05|race-chu05|vet-chu05|benchmark-chu05|audit-chu05)' Makefile
rg -n '^test:' Makefile
