#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestCommandJournalCrashFaultMatrixKeepsDurablePrefix$' -count=1
