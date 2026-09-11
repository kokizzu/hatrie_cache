#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournal(SubscriptionReplayMixed100|SpaceSubscriptionReplay50Of100)$' -benchmem -count=5
