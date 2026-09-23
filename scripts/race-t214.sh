#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache ./hat/hatJournal -run '^TestT214SnapshotStream'
