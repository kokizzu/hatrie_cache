#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'Test.*Backup|Test.*Restore|TestCommandJournal|TestT042' -count=1
