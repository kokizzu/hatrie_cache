#!/bin/sh
set -eu

git diff --check
go test ./hat/hatSql
