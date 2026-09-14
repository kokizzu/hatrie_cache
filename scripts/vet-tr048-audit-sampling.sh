#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatAudit ./hat/hatCache ./cmd/hatrie-cache
