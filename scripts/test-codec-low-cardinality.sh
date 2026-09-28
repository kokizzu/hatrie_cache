#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run 'TestLowCardinality' -count=1
