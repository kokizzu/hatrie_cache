#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./cmd/hatrie-cache -count=1
