#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCH050RowBinaryPayloadIsSmallerThanJSONRows$' -count=1 -v
