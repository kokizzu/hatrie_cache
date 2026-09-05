#!/usr/bin/env bash
set -euo pipefail

go vet ./hat/hatCache ./hat/hatJournal ./cmd/hatrie-cache
