#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestTT012' -count=1
