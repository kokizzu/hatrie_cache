#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^$' -bench '^BenchmarkCH026PhraseBaseline$' -benchmem -count=5
go test ./hat/hatCache -run '^$' -bench '^BenchmarkCH026PhraseIndexed$' -benchmem -count=5
