#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestPartitionedCommandJournal' -count=1
