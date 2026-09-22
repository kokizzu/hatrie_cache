#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestT201' -count=1
