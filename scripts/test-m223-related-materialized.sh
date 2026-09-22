#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestM223|TestM222|TestM220|TestM219|TestM218|TestMaterialized)' -count=1
