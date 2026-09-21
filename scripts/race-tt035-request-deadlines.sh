#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCommand ./hat/hatCache ./cmd/hatrie-cache -run '^TestTT035' -count=1
