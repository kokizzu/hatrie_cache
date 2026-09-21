#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatCache -run '^TestMZ050' -count=1
go test ./hat/hatCache -count=1
