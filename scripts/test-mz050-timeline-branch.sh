#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestMZ050' -count=1
