#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^Test(CompiledSQLNativeDataflowTripleGroupedOrdered|AutomaticNativeDataflowTripleGroupedOrdered)' -count=1
