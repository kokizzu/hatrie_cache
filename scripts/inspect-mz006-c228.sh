#!/usr/bin/env bash
set -euo pipefail

printf '%s\n' 'Backup manifest/object-store files:'
rg --files hat/hatBackup | rg 'catalog|manifest|object|store|chain|retention|gc'
printf '%s\n' '' 'Backup interfaces/types:'
rg -n -C 4 'type (ObjectStore|BundleManifest|BundleFile|BackupChainPlan)|func (PlanBackupChain|.*ObjectKey|.*fileObjectKey|.*collect)' hat/hatBackup --glob '*.go'
printf '%s\n' '' 'object_store.go interfaces and layout:'
sed -n '27,155p' hat/hatBackup/object_store.go
printf '%s\n' '' 'chain.go retention plan:'
sed -n '1,175p' hat/hatBackup/chain.go
