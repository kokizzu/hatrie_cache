#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestMU34' -count=1
