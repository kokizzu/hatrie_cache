#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'TestCommandJournalSpaceSubscription|TestCommandJournalSubscription' -count=1
