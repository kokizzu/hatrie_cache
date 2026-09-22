#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestCH004FinalSchemaRegistry|^TestCH004Final' -count=5 -timeout=3m
