#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run 'Test(MutableIncrementalRankWindow|IncrementalRankWindow)' -count=10
