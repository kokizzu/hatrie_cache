#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatBackup ./hat/hatCache ./cmd/hatrie-cli
