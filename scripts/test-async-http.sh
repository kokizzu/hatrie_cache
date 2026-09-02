#!/bin/sh
set -eu

go test ./hat/hatCache ./cmd/hatrie-cache \
	-run 'Test(ParseConfigMonitoringAsyncCommands|CommandJournalSubmitAsyncCommand)' \
	-count=1
