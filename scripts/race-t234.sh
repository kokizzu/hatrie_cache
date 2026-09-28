#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'SQLTransaction|T234' -count=1
