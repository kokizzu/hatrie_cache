#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestTU34' -count=1
go test ./hat/hatJournal -run 'SpaceSyncPolicy' -count=1
