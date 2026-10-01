#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatCache -count=1
