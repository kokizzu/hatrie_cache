#!/usr/bin/env bash
set -euo pipefail

git diff --check
go test ./hat/hatCache -run '^TestCommandJournalSink' -count=1
