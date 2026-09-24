#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -count=1
go test -race ./hat/hatSchema -run 'TestTT027' -count=1
go vet ./hat/hatSchema
