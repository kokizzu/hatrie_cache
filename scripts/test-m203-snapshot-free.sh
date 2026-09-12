#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCommandJournalSubscriptionSkipReplayStartsAtCurrentTail$' -count=1
