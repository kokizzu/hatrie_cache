#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestT042Replay' -count=1
