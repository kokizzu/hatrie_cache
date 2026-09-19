#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -count=1
go test -race ./hat/hatTopology -count=1
go vet ./hat/hatTopology
bash scripts/benchmark-tu27.sh
