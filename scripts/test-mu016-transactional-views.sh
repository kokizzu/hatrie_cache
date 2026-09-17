#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLSessionTransactionalViewChangesPublishOneVersion|TestSQLSessionCreateOrReplaceViewDDLUsesTransactionalBoundary' -count=1
