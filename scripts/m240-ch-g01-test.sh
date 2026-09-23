#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run '^TestCH001PartialMergeJoinPreservesDuplicatesAndNullSemantics$' -count=1
