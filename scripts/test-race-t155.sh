#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run '^TestCommandAllocationBudgets$' -count=1
