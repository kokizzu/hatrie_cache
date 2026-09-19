#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatWorkload -count=1
go test -race ./hat/hatWorkload -count=1
go vet ./hat/hatWorkload
