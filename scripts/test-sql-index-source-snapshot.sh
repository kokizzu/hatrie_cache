#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^(TestSQLJSONSourceStringSnapshotSurvivesReplacement|TestHatTrieOptionalSQLJSONFieldIndexRefreshesAndPlansIndexScan|TestSQLJSONCoveringIndexRefreshesAfterStringReplacement)$' -count=1
