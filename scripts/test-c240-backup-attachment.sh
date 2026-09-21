#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestC240' -count=1
