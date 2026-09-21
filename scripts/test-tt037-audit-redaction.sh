#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatAudit -run '^TestTT037' -count=1
