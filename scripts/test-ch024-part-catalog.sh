#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle -run '^TestCH024' -count=1
