#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCodec -run 'TestDefaultValueSuppression' -count=1
