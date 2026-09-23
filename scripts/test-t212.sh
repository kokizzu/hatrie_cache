#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestT212' -count=1
