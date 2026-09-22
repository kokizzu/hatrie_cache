#!/usr/bin/env bash
set -euo pipefail

go test -tags=t212 ./hat/hatJournal ./hat/hatCache -count=1
