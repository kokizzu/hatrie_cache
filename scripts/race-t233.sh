#!/usr/bin/env bash
set -euo pipefail
go test -race ./hat/hatCache -run '^TestT233' -count=1
