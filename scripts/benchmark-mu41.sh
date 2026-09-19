#!/usr/bin/env bash
set -euo pipefail

go test -tags mu41 ./hat/hatSql -run '^$' -bench '^BenchmarkMU41WebhookEvent' -benchmem -count=5
