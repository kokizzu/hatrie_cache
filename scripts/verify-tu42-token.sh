#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPagination -count=1
go test -race ./hat/hatPagination -count=1
go vet ./hat/hatPagination
