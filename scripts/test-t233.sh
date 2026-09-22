#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^TestT233' -count=1
