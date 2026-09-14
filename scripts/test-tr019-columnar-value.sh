#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestTR019ColumnarValuePreservesPhysicalPrecedence$' -count=1
