#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'TestCHU23|TestCommandJournalSubmitAsyncCommand|TestMonitoringAsyncCommand' -count=1
