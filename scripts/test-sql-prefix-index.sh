#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestSQLJSONFieldIndex(AcceleratesLikePrefix|LikePrefixFallsBackForUnsafePatternsAndTypes|LikePrefixRefreshesAfterReplacement)$' -count=1
