#!/usr/bin/env bash
set -euo pipefail

go test ./cmd/hatrie-cache -run '^TestParseConfig' -count=1
