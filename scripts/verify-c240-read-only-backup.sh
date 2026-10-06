#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatBackup
go test -race ./hat/hatBackup -run 'TestC240' -count=1
go vet ./hat/hatBackup
