#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSpill -count=1
go test -race ./hat/hatSpill -count=1
go vet ./hat/hatSpill
