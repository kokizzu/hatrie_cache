#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatCache -run '^TestT232' -count=1
