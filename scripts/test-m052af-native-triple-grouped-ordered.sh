#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^Test(CompiledSQLNativeDataflowTripleGroupedOrdered|AutomaticNativeDataflowTripleGroupedOrdered)' -count=1
