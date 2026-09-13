#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestMonitoringHandlerJournalCursorResumesAndBinds$' -count=1
