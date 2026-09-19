#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

stage_dir="$(mktemp -d /tmp/hatrie-cache-m053-baseline.XXXXXX)"
archive="$stage_dir/origin.tar"
cleanup() {
  rm -rf "$stage_dir"
}
trap cleanup EXIT

mkdir -p "$stage_dir/tree/hat/hatSql"
git archive --format=tar --output="$archive" origin/master
tar -xf "$archive" -C "$stage_dir/tree"
cat > "$stage_dir/tree/hat/hatSql/m_u03_external_snapshot_ingestion_benchmark_test.go" <<'EOF'
package hatSql_test

import (
  "testing"

  "hatrie_cache/hat/hatSql"
)

func BenchmarkSQLExternalSnapshotControl(b *testing.B) {
  rows := make([]hatSql.Row, 128)
  for index := range rows {
    rows[index] = hatSql.Row{"id": int64(index), "name": "order", "total": float64(index) / 10, "active": index%2 == 0}
  }
  sources := hatSql.NewVirtualSources()
  if err := sources.Register("orders", hatSql.VirtualSourceFunc(func() ([]hatSql.Row, error) { return rows, nil })); err != nil {
    b.Fatal(err)
  }
  b.ReportAllocs()
  b.ResetTimer()
  for index := 0; index < b.N; index++ {
    if _, err := sources.ResolveSQLVirtualSource("orders"); err != nil {
      b.Fatal(err)
    }
  }
}
EOF
cd "$stage_dir/tree"
go test ./hat/hatSql -run '^$' -bench '^BenchmarkSQLExternalSnapshotControl$' -benchmem -count=5
