#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run '^TestMaterializedSource(BuildsSecondaryIndexOnline|OnlineIndexBuildKeepsConcurrentInserts|SQLResolverAdapterUsesOnlyMaintainedSecondaryIndexes)$' -count=1
