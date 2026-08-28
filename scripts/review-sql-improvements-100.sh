#!/usr/bin/env sh
set -eu

make verify-sql-improvements-100
git diff --check
git diff -- Makefile
git status --short
printf '%s\n' 'SQL improvements backlog review passed'
