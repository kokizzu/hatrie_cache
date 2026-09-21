#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatMerkle -run '^TestCH024' -count=1
