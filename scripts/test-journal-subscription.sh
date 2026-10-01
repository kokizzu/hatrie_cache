#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatCache \
	-run '^TestCommandJournalSubscription' \
  -count=1
