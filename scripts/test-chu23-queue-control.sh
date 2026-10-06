#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestCHU23' -count=1
