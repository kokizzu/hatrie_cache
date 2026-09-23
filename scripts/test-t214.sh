#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT214SnapshotStream' -count=1
