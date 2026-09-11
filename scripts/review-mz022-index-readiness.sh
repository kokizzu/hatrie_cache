#!/usr/bin/env bash
set -euo pipefail

git diff --check
go vet ./hat/hatCache ./hat/hatSql
