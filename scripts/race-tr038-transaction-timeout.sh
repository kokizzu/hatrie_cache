#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'TestTR038' -count=1
