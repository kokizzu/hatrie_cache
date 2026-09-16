#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestC227' -count=1
