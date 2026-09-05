#!/bin/sh
set -eu

case "${1:-test}" in
format)
	gofmt -w hat/hatCache/journal_replay_fastpath.go hat/hatCache/journal_replay_fastpath_test.go
	;;
test)
	go test ./hat/hatCache -run '^TestCommandJournalReplay' -count=1
	;;
race)
	go test -race ./hat/hatCache -run '^TestCommandJournalReplay' -count=1
	;;
bench)
	go test -v ./hat/hatCache -run '^$' -bench '^BenchmarkCommandJournalReplayFastPath$' -benchmem -benchtime=100ms -count=3
	;;
*)
	printf '%s\n' 'usage: test-journal-replay-fastpath.sh [format|test|race|bench]' >&2
	exit 2
	;;
esac
