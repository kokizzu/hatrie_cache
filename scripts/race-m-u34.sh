#!/usr/bin/env bash
set -euo pipefail

exec go test -race ./hat/hatCache \
	-run '^TestCommandJournalSubscriptionCheckpoint' \
  -count=1
