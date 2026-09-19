#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatIndexStats -count=1
go test -race ./hat/hatIndexStats -count=1
go vet ./hat/hatIndexStats
