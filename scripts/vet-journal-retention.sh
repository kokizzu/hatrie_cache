#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatJournal ./hat/hatCache ./cmd/hatrie-cache
