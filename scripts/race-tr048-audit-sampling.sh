#!/usr/bin/env bash
set -euo pipefail

go test -race -count=1 ./hat/hatAudit ./hat/hatCache ./cmd/hatrie-cache
