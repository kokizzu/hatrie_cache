#!/usr/bin/env bash
set -euo pipefail

git diff --check
go vet ./hat/hatSql
printf '%s\n' 'MZ-015 diff and hatSql vet checks passed.'
