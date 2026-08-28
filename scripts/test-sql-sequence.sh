#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^TestMatchOrderedEventSequence' -count=1
