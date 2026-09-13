#!/usr/bin/env bash
set -eu

go test ./hat/hatSql -run 'TestMZ025' -count=1
