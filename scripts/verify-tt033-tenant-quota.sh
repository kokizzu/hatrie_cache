#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatFiber -count=1
go test -race ./hat/hatFiber -count=1
go vet ./hat/hatFiber
