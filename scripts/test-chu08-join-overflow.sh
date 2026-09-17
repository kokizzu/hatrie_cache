#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestC229' -count=1
