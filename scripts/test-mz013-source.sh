#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache \
	-run '^TestCommandJournalSourceCheckpoint' \
	-count=1
