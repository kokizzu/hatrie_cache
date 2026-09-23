#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT213ScheduledSnapshot' -count=1
