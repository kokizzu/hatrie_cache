#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go vet ./hat/hatJournal ./hat/hatCache
