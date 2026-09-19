#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMembership -count=1
go test -race ./hat/hatMembership -count=1
go vet ./hat/hatMembership
