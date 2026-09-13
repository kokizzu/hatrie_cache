#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestQuerySubscriptionsExportSnapshotsAt' -count=1
