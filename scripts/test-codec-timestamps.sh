#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run 'TestCompactTimestamp' -count=1
