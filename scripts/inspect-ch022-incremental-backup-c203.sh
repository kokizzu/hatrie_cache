#!/usr/bin/env bash
set -euo pipefail

printf 'Backup and restore files:\n'
rg --files hat/hatBackup hat/hatSnapshot cmd | sort
printf 'Object-store target options:\n'
sed -n '45,90p' hat/hatBackup/encryption.go
printf 'Backup declarations:\n'
rg -n '^type |^const |^var |^func |^func \(' hat/hatBackup/*.go hat/hatSnapshot/*.go
printf 'Object-store implementation:\n'
sed -n '450,650p' hat/hatBackup/object_store.go
printf 'CH-022 catalog entries:\n'
rg -n -A 2 -B 2 'CH-022|incremental part backup|content-addressed part' ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md
printf 'Feature Makefile region:\n'
sed -n '14250,14345p' Makefile
printf 'CH-022 test store:\n'
sed -n '10,65p' hat/hatBackup/ch022_incremental_part_backup_test.go
printf 'Working tree:\n'
git status --short
