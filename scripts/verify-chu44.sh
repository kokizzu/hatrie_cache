#!/usr/bin/env bash
set -euo pipefail

go test -run 'TestCHU44SQLAggregateState' ./hat/hatSql
go test -race -run 'TestCHU44SQLAggregateState' ./hat/hatSql
go vet ./hat/hatSql
