#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^Test(CompiledSQLNativeDataflowTripleGroup|AutomaticNativeDataflowTripleGroup)' -count=1
