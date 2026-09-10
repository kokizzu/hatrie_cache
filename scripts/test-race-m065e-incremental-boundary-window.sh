#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestIncrementalBoundaryWindow' -count=1
