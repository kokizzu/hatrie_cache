#!/bin/sh
set -eu

go test -tags tg42 ./hat/hatCache -run '^TestCommandJournalSubscription(KeyPrefix|Coalesces)' -count=1
