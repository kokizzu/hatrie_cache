#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestT208|^TestT207|^TestT206' -count=1
