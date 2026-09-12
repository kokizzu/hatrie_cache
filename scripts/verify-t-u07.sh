#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -count=1
go test -race ./hat/hatReplication -count=1
go vet ./hat/hatReplication
