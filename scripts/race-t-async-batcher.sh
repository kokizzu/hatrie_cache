#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPipeline -run '^TestAsyncBatcher' -count=1
