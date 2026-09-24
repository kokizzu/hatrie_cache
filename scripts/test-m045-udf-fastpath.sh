#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestM045DeterministicLiteralUDFIsEvaluatedOncePerBatch$' -count=1
