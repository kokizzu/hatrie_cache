#!/usr/bin/env bash
set -euo pipefail

exec env GOMAXPROCS=1 go test -race ./hat/hatAuth -run '^TestRoleCatalog' -count=1
