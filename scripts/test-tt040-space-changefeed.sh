#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestCommandJournalSpaceSubscription' -count=1
