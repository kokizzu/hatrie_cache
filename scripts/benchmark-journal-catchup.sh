#!/usr/bin/env sh
set -eu

package=${PACKAGE:-.}
count=${COUNT:-1}
benchtime=${BENCHTIME:-5x}
bench=${JOURNAL_CATCHUP_BENCH:-BenchmarkJournalCatchUpDeltaVsFullSnapshot}

set -- go test -run '^$' -bench "$bench" -benchmem -count "$count"
set -- "$@" -benchtime "$benchtime"
set -- "$@" "$package"

printf '+'
for arg do
	printf ' %s' "$arg"
done
printf '\n'

exec "$@"
