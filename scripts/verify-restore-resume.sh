#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup ./hat/hatCache ./cmd/hatrie-cli -count=1
go vet ./hat/hatBackup ./hat/hatCache ./cmd/hatrie-cli
