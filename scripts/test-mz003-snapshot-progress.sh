#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^TestMZ003' -count=1 -v
