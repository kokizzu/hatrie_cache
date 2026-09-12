#!/bin/sh
set -eu

go test ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalSubscription(ExactKeyReplayBaseline|KeyPrefixReplay|PrefixReplay(Uncoalesced|Coalesced))$' -benchmem -benchtime=500ms -count=3
