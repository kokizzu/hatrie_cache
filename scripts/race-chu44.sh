#!/usr/bin/env bash
set -euo pipefail

go test -race -run 'TestCHU44SQLAggregateState' ./hat/hatSql
