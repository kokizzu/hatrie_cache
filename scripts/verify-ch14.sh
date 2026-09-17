#!/bin/sh
set -eu

go test ./hat/hatSql
go test -race ./hat/hatSql -run 'SQLMutationDependencyGraph'
go vet ./hat/hatSql
git diff --check
