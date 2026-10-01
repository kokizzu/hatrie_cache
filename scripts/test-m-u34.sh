#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatCache \
	-run '^TestCommandJournalSubscriptionCheckpoint' \
  -count=5
