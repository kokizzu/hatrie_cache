#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -count=1 -run '^TestCompiledSQLAutomaticNativeCompositeGrouped'
