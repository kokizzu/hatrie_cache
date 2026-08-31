#!/bin/sh
set -eu

case "${MODE:-}" in
  journal)
    rg -n 'Compaction|compaction|CompactedThrough|outboxRetain|Tail\(' hat/hatCache/journal.go hat/hatCache/journal_segments.go hat/hatJournal
    rg -n 'func \(journal \*CommandJournal\).*Compact' hat/hatCache/journal.go hat/hatCache/journal_segments.go
    rg -n 'compactLocked\(' hat/hatCache/journal.go hat/hatCache/journal_segments.go
    ;;
  journal-impl)
    sed -n '1,90p' hat/hatCache/journal.go
    sed -n '240,320p' hat/hatCache/journal_segments.go
    sed -n '990,1030p' hat/hatCache/journal.go
    sed -n '1320,1545p' hat/hatCache/journal.go
    ;;
  table)
    rg -n 'type .*Table|type .*Change|type .*Stream|IncrementalProjection|Subscription|MaterializedView' hat/hatSql hat/hatCache
    ;;
  events)
    sed -n '1,260p' hat/hatSql/events.go
    sed -n '1,260p' hat/hatSql/subscription.go
    ;;
  segment)
    rg -n 'RowsPerSegment|StringBloom|BloomSegment|NumericSegments|segment' hat/hatSql hat/hatCache/sql_query.go
    ;;
  join)
    rg -n 'HashJoin|hash join|Join.*Plan|executeSQL.*Join|Bloom' hat/hatSql/query.go hat/hatSql/contracts.go
    ;;
  *)
    printf '%s\n' 'expected MODE: journal, journal-impl, table, events, segment, or join' >&2
    exit 2
    ;;
esac
