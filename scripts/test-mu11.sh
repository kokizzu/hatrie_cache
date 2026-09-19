#!/usr/bin/env bash
set -euo pipefail

go test -tags mu11 ./hat/hatSql -run 'TestTypedTable(AggregateArrangementAdvisor|ArrangementAdvisor)' -count=1 "$@"
