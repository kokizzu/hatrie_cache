#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPrimaryPruning -count=1
go test -race ./hat/hatPrimaryPruning -count=1
go vet ./hat/hatPrimaryPruning
