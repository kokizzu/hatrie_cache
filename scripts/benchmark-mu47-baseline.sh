#!/usr/bin/env bash
set -euo pipefail

go test -tags=mu47baseline ./hat/hatSql -run '^$' -bench '^BenchmarkQuerySubscriptionProgressFrameBaseline' -benchmem -count=5
