#!/usr/bin/env bash
set -euo pipefail

go test -v ./hat/hatSql -run 'Test(CompiledSQLNativeDataflowQuadGroupedOrdered|AutomaticNativeDataflowQuadGroupedOrdered)' -count=1
