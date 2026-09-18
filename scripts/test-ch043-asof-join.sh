#!/usr/bin/env bash
set -eu
go test ./hat/hatSql -run 'TestCH043AsOfJoin' -count=1
