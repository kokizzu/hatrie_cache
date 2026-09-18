#!/usr/bin/env bash
set -euo pipefail

test -s MZ021_SINK_IDEMPOTENCY_TOKENS.md
rg -q 'MZ021_SINK_IDEMPOTENCY_TOKENS.md' README.md
rg -q 'DeriveSQLSinkIdempotencyKey' MZ021_SINK_IDEMPOTENCY_TOKENS.md
rg -q '## MZ-021: Sink idempotency tokens' BENCHMARK.md
rg -q 'MZ-21.*\[x\]' INSPIRATION_BACKLOG.md
printf '%s\n' 'MZ-021 documentation links and benchmark markers verified.'
