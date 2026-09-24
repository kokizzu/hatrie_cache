#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMU031AggregateTransaction' -count=1
