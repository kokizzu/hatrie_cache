#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestCH004FinalSchemaRegistry' -count=1 -timeout=20s
