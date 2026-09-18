#!/usr/bin/env bash
set -eu

go test -race ./hat/hatSql -run '^TestCH043AsOfJoin' -count=1
