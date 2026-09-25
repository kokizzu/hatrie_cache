#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Test(FileSQLProjectionDefinitionStoreRoundTrip|SQLSessionRestoresDurableProjectionDefinitions)$' -count=1
