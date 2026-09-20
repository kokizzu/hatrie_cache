#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestTU39' -count=1
