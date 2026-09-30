#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestCommandJournal|TestT042' -count=1
