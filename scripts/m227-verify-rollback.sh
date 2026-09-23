#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestCommandJournal.*Replay|TestReplay.*CommandJournal' -count=1
