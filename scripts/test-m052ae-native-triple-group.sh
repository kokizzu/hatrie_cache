#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^Test(CompiledSQLNativeDataflowTripleGroup|AutomaticNativeDataflowTripleGroup)' -count=1
