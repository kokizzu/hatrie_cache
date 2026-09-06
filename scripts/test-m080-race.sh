#!/bin/sh
set -eu

go test -race ./hat/hatSql -count=1
