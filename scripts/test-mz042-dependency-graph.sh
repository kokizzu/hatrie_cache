#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMaterializedViews(BuildDependencyInvalidationIndex|DependencyInvalidationDeduplicatesOverlappingChanges)$' -count=1
