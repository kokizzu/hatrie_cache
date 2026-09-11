#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestPreview' -count=1
