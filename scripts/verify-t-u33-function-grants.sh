#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatAuth -count=1
go test -race ./hat/hatAuth -count=1
go vet ./hat/hatAuth
