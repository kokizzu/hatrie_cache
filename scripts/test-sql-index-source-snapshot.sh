#!/usr/bin/env sh
set -eu

go test ./hat/hatCache -run '^(TestSQLJSONSourceStringSnapshotSurvivesReplacement|TestHatTrieOptionalSQLJSONFieldIndexRefreshesAndPlansIndexScan)$' -count=1
