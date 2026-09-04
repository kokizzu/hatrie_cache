#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCommandAllocationBudgets$' -count=1 -v
