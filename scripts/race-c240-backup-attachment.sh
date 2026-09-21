#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -race -run '^TestC240' -count=1
