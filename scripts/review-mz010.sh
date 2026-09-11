#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatCache -run 'TestCommandJournalSubscription' -count=1
