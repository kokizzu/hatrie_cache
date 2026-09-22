#!/usr/bin/env bash
set -euo pipefail

go test -race -tags=t212 ./hat/hatJournal ./hat/hatCache -run '^TestT212' -count=1
