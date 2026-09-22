#!/usr/bin/env bash
set -euo pipefail

repo_root=$(pwd)
parent_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m247-parent.XXXXXX")
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
  'var m247ParentExpiredErrorSink error' \
  'var m247ParentExpiredMessageSink string' \
  'var m247ParentValidTimestampErrorSink error' \
  '' \
  'func BenchmarkM247AcquireExpired(b *testing.B) {' \
  '  frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = frontiers.Close() })' \
  '  if err := frontiers.Register("orders"); err != nil { b.Fatal(err) }' \
  '  if err := frontiers.Advance("orders", 100, 120); err != nil { b.Fatal(err) }' \
  '  retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = retention.Close() })' \
  '  b.ReportAllocs()' \
  '  b.ResetTimer()' \
  '  for i := 0; i < b.N; i++ {' \
  '    _, m247ParentExpiredErrorSink = retention.Acquire("orders", 99)' \
  '  }' \
  '}' \
  '' \
  'func BenchmarkM247AcquireExpiredMessage(b *testing.B) {' \
  '  frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = frontiers.Close() })' \
  '  if err := frontiers.Register("orders"); err != nil { b.Fatal(err) }' \
  '  if err := frontiers.Advance("orders", 100, 120); err != nil { b.Fatal(err) }' \
  '  retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = retention.Close() })' \
  '  b.ReportAllocs()' \
  '  b.ResetTimer()' \
  '  for i := 0; i < b.N; i++ {' \
  '    _, err := retention.Acquire("orders", 99)' \
  '    m247ParentExpiredMessageSink = err.Error()' \
  '  }' \
  '}' \
  '' \
  '' \
  'func BenchmarkM247CheckTimestampValid(b *testing.B) {' \
  '  frontiers, err := NewFrontierRegistry(FrontierRegistryOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = frontiers.Close() })' \
  '  if err := frontiers.Register("orders"); err != nil { b.Fatal(err) }' \
  '  if err := frontiers.Advance("orders", 100, 120); err != nil { b.Fatal(err) }' \
  '  retention, err := NewFrontierRetentionRegistry(frontiers, FrontierRetentionOptions{})' \
  '  if err != nil { b.Fatal(err) }' \
  '  b.Cleanup(func() { _ = retention.Close() })' \
  '  b.ReportAllocs()' \
  '  b.ResetTimer()' \
  '  for i := 0; i < b.N; i++ {' \
  '    m247ParentValidTimestampErrorSink = retention.checkTimestamp("orders", 100)' \
  '  }' \
  '}' > "$parent_dir/hat/hatPipeline/m247_parent_benchmark_test.go"

printf '%s\n' '== M246 parent =='
(cd "$parent_dir" && go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkM247(AcquireExpired|AcquireExpiredMessage|CheckTimestampValid)$' -benchmem -count=5)
printf '%s\n' '== M247 current =='
go test ./hat/hatPipeline -run '^$' -bench '^BenchmarkM247(AcquireExpired|AcquireExpiredMessage|CheckTimestampValid)$' -benchmem -count=5
