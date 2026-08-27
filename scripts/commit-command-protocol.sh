#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
git diff --check
git add Makefile hat/hatCommand/protocol.go hat/hatCommand/protocol_test.go hat/hatCache/command.go hat/hatCache/monitoring.go hat/hatCache/command_protocol_test.go scripts/test-command-protocol.sh scripts/format-command-protocol.sh scripts/commit-command-protocol.sh scripts/push-command-protocol.sh
git diff --cached --check
git commit -m "feat: negotiate command protocol versions"
