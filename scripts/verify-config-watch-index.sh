#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -count=1
go test -race ./hat/hatTopology -run 'ConfigWatch' -count=1
go vet ./hat/hatTopology
