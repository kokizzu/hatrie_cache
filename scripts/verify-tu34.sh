#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatJournal -count=1
go test -race ./hat/hatJournal -count=1
go vet ./hat/hatJournal
bash scripts/benchmark-tu34.sh
