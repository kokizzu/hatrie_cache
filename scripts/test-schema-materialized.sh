#!/usr/bin/env sh
set -eu

go test ./hat/hatSchema -run '^TestMaterializedSource'
