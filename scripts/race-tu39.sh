#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestTU39' -count=1
