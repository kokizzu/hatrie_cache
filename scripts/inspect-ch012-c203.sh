#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' '== delete and tombstone surfaces =='
rg -n 'Deleted|deleted|Tombstone|tombstone|Row.*Valid|valid.*row|Delete|delete' hat/hatSql/typed_table*.go hat/hatSql/columnar*.go hat/hatCache/sql_*.go | head -n 320 || true
printf '%s\n' '== storage lifecycle declarations =='
rg -n 'type .*Storage|type .*Part|type .*Segment|func .*Compact|func .*Merge|Compaction' hat/hatSql/typed_table*.go hat/hatSql/columnar*.go hat/hatCache/sql_*.go | head -n 260 || true
printf '%s\n' '== existing CH-012 documents and targets =='
rg -n 'CH-012|CH012|delete bitmap|Delete Bitmap' --glob '*.go' --glob '*.md' --glob 'Makefile' . || true
printf '%s\n' '== patch-part layout =='
sed -n '1,145p' hat/hatSql/typed_table_patch_parts.go
printf '%s\n' '== delete/update integration =='
sed -n '450,570p' hat/hatSql/typed_table.go
printf '%s\n' '== existing patch tests and constructors =='
sed -n '1,180p' hat/hatSql/typed_table_patch_parts_test.go
rg -n 'type TypedTableOptions|PatchParts|NewTypedTable' hat/hatSql/typed_table.go hat/hatSql/typed_table*.go | head -n 160 || true
