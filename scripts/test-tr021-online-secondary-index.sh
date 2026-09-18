#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestMaterializedSource(BuildsSecondaryIndexOnline|OnlineIndexBuildKeepsConcurrentInserts)$' -count=1
