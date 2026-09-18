#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLMutationDependencyQueue' -count=1
