#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT042Replay' -count=1
