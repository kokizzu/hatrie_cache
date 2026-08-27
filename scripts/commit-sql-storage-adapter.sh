#!/bin/sh
set -eu

git add Makefile hat/hatStorage/sql_adapter.go hat/hatStorage/sql_adapter_test.go scripts/commit-sql-storage-adapter.sh scripts/format-sql-storage-adapter.sh scripts/push-sql-storage-adapter.sh scripts/test-sql-storage-adapter.sh
git commit -m 'feat: add pluggable SQL storage adapters'
