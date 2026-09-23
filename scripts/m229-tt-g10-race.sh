#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestTTG10' -race -count=1
