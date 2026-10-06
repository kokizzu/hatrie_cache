#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestCommandJournalSubmitAsyncCommand|TestMonitoringAsyncCommand' -count=1
