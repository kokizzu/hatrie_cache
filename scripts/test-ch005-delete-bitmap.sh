#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle -run '^TestCH005' -count=1
