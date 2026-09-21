#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestMZ050' -count=1
