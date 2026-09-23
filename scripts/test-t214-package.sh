#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache ./hat/hatJournal -run 'TestT214SnapshotStream|TestSnapshot'
