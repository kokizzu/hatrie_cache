#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -count=1 -run '^TestCompiledSQLAutomaticNativeDataflow'
