#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestSQLSessionTransactionalViewChangesPublishOneVersion|TestSQLSessionCreateOrReplaceViewDDLUsesTransactionalBoundary' -count=1
