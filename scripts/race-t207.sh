#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestT207' -count=1
