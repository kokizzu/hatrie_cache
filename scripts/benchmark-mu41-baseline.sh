#!/usr/bin/env bash
set -euo pipefail

go test -tags mu41baseline ./hat/hatSql -run '^$' -bench '^BenchmarkMU41WebhookFingerprintBaseline$' -benchmem -count=5
