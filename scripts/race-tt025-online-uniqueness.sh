#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestTT025' -count=1
