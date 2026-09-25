#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCompiledSQLAutomaticNativeDataflow' -count=1
