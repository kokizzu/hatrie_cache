#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestT043' -count=1
