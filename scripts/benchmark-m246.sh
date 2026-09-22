#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
parent_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m246-parent.XXXXXX")
cleanup() {
	git -C "$repo_root" worktree remove --force "$parent_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

git -C "$repo_root" worktree add --detach "$parent_dir" HEAD^ >/dev/null
printf '%s\n' \
  'package hatPipeline' \
  '' \
  'import "testing"' \
  '' \
  'var m246ParentFrontierRetentionSnapshotSink FrontierRetentionSnapshot' \
  '' \
  'func BenchmarkM246FrontierRetentionSnapshotNoPolicy(b *testing.B) {' \
  '  frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = frontiers.Close() })' \
  '  if err := frontiers.Register("events"); err != nil { b.Fatal(err) }' \
  '  if err := frontiers.Advance("events", 10, 100); err != nil { b.Fatal(err) }' \
  '  retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = retention.Close() })' \
  '  b.ResetTimer()' \
  '  for i := 0; i < b.N; i++ {' \
  '    snapshot, err := retention.Snapshot("events")' \
  '    if err != nil { b.Fatal(err) }' \
  '    m246ParentFrontierRetentionSnapshotSink = snapshot' \
  '  }' \
  '}' > "$parent_dir/hat/hatPipeline/m246_parent_benchmark_test.go"
benchmark='^(BenchmarkMZ04FrontierRetentionSnapshot|BenchmarkM246FrontierRetentionSnapshotNoPolicy|BenchmarkM246FrontierRetentionSnapshotWithPolicy|BenchmarkM246FrontierRetentionSetUsage)$'

printf '%s\n' '== M245 parent =='
(cd "$parent_dir" && go test ./hat/hatPipeline -run '^$' -bench "$benchmark" -benchmem -count=5)
printf '%s\n' '== M246 current =='
go test ./hat/hatPipeline -run '^$' -bench "$benchmark" -benchmem -count=5
