#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./cmd/hatrie-cache -run '^TestTR004' -count=1
