#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestTR038' -count=1
