#!/usr/bin/env bash
set -euo pipefail

for file in CH022_INCREMENTAL_PART_BACKUP.md ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md README.md BENCHMARK.md; do
  test -f "$file"
done
rg -q 'ManifestCatalog' CH022_INCREMENTAL_PART_BACKUP.md
rg -q 'optional durable.*BackupManifestCatalog' ENGINE_IDEAS.md
rg -q 'optional durable chain catalog' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'durable manifest catalog' README.md
rg -q 'CH-022 Durable Manifest Catalog' BENCHMARK.md

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT
git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatBackup/ch022_catalog_integration_test.go "$tmp_dir/hat/hatBackup/"
cp hat/hatBackup/encryption.go "$tmp_dir/hat/hatBackup/"
cp hat/hatBackup/object_store.go "$tmp_dir/hat/hatBackup/"
(cd "$tmp_dir" && go test ./hat/hatBackup -run '^Test(CH022|Test)' -count=1 -timeout 30s)
(cd "$tmp_dir" && go test -race ./hat/hatBackup -run '^TestCH022ObjectStoreBackupAppendsConfiguredManifestCatalog$' -count=1 -timeout 30s)
(cd "$tmp_dir" && go vet ./hat/hatBackup)
