#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestMaterialized|TestM217|TestM218|TestM219|TestM220)' -count=1
