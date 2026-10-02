#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestTU21' -count=1 -timeout=120s
