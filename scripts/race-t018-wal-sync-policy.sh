#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache" go test -race ./hat/hatJournal ./hat/hatCache -run '^TestT018' -count=1
