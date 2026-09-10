#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run 'Test(MutableIncrementalRankWindow|IncrementalRankWindowApplyRequiresOptIn)' -count=1
