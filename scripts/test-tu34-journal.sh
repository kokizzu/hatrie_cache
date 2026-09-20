#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatJournal -run 'SpaceSyncPolicy' -count=1
