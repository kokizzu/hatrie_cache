#!/usr/bin/env bash
set -u

repo_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"

printf '%s\n' '' '== capability evidence files =='
for label in \
  'segment skip / zone map' \
  'bitmap index' \
  'covering index / row id' \
  'vectorized / late materialization' \
  'Top-N / LIMIT pushdown' \
  'materialized / incremental projection' \
  'subscription / changefeed' \
  'partition pruning' \
  'async insert / group commit' \
  'WAL / LSN / incremental backup / PITR' \
  'MVCC / transaction' \
  'Vinyl / LSM / disk backed'; do
  case "$label" in
    'segment skip / zone map') pattern='segment.?skip|zone.?map' ;;
    'bitmap index') pattern='bitmap.?index' ;;
    'covering index / row id') pattern='covering.?index|row.?id|row.?offset' ;;
    'vectorized / late materialization') pattern='vector|late.?material|expression.?batch' ;;
    'Top-N / LIMIT pushdown') pattern='top.?n|limit.?push' ;;
    'materialized / incremental projection') pattern='materialized|incremental.?projection' ;;
    'subscription / changefeed') pattern='subscription|tail|change.?event|change.?feed' ;;
    'partition pruning') pattern='partition.?prun' ;;
    'async insert / group commit') pattern='async.*insert|group.?commit' ;;
    'WAL / LSN / incremental backup / PITR') pattern='wal|lsn|incremental.?backup|point.?in.?time' ;;
    'MVCC / transaction') pattern='mvcc|transaction' ;;
    'Vinyl / LSM / disk backed') pattern='vinyl|lsm|disk.?backed' ;;
  esac
  if rg -q -i "$pattern" --glob '*.md' --glob '*.go' .; then
    printf '%s\n' "PRESENT  $label"
  else
    printf '%s\n' "ABSENT   $label"
  fi
done

printf '%s\n' '' '== adoption and design-document headings =='
for doc in \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  PROJECTION_ADVISOR.md \
  INCREMENTAL_PROJECTIONS.md \
  REFRESH_SCHEDULER.md \
  PROJECTION_FRONTIERS.md \
  PARTITIONING_PROPOSAL.md \
  TYPED_TABLES.md \
  TYPED_TABLE_ARRANGEMENTS.md \
  BENCHMARK.md; do
  if test -f "$doc"; then
    printf '%s\n' "--- $doc"
    rg -n '^#' "$doc"
  fi
done

printf '%s\n' '' '== documented deferred work =='
rg -n -A 18 '^## Deliberately Deferred|^### MVCC|^### Immutable Parts|^### More Generic SQL Rewrites' \
  ADOPTED_QUERY_ENGINE_IDEAS.md

printf '%s\n' '' '== documented implementation status =='
rg -n -A 12 '^## Latest Optimization Spot Check|^## SQL Columnar|^## SQL Shared Index|^## SQL Index Source|^## Journal-Driven|^## Raw Results' \
  BENCHMARK.md

printf '%s\n' '' '== related Makefile targets =='
if rg -n \
  '^(audit|test|benchmark|verify|deliver|commit|push).*(segment|bitmap|covering|vector|topn|limit|projection|subscription|event|material|partition|backup|restore|replication|wal|transaction|persistence|storage|typed-table).*:' \
  Makefile; then
  :
fi

printf '%s\n' '' '== current worktree ownership =='
git status --short

printf '%s\n' '' '== recent commits =='
git log --oneline -20

printf '%s\n' '' '== Makefile tail =='
tail -n 80 Makefile

case "${DETAIL:-}" in
  typed-table)
    printf '%s\n' '' '== typed table implementation =='
    sed -n '1,420p' hat/hatSql/typed_table.go
    printf '%s\n' '' '== typed table arrangements =='
    sed -n '1,360p' hat/hatSql/typed_table_arrangements.go
    ;;
  storage)
    printf '%s\n' '' '== storage implementation =='
    sed -n '1,360p' hat/hatStorage/engine.go
    printf '%s\n' '' '== cache persistence implementation =='
    sed -n '1,360p' hat/hatCache/pebble_store.go
    ;;
  query)
    printf '%s\n' '' '== materialized and incremental query implementation =='
    sed -n '1,320p' hat/hatSql/materialized.go
    sed -n '1,280p' hat/hatSql/incremental_projection.go
    printf '%s\n' '' '== columnar query implementation =='
    sed -n '1,360p' hat/hatSql/query.go
    ;;
  typed-table-symbols)
    printf '%s\n' '' '== typed table symbols =='
    rg -n '^type |^func ' hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go
    printf '%s\n' '' '== version, snapshot, and changefeed references =='
    rg -n -i 'snapshot|version|change|compact|reader|lock|checkpoint|sequence' hat/hatSql/typed_table.go hat/hatSql/typed_table_arrangements.go
    ;;
  mvcc)
    printf '%s\n' '' '== columnar batch contracts =='
    rg -n '^type Columnar|^func .*Columnar|type .*SourceResolver' hat/hatSql
    printf '%s\n' '' '== current typed table source methods =='
    sed -n '420,590p' hat/hatSql/typed_table.go
    printf '%s\n' '' '== current snapshot contracts =='
    sed -n '1,260p' hat/hatSql/snapshot.go
    ;;
  mvcc-docs)
    printf '%s\n' '' '== adoption document =='
    sed -n '1,100p' ADOPTED_QUERY_ENGINE_IDEAS.md
    printf '%s\n' '' '== typed table document =='
    sed -n '145,270p' TYPED_TABLES.md
    printf '%s\n' '' '== benchmark document tail =='
    tail -n 100 BENCHMARK.md
    ;;
  makefile)
    tail -n 100 Makefile
    ;;
  delivery)
    sed -n '1,260p' scripts/deliver-sql-temporal-storage.sh
    ;;
esac
