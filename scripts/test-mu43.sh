#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestSinkBackpressure' -count=1
