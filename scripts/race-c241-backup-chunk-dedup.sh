#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestC241' -count=1
