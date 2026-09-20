#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestMU34' -count=1
