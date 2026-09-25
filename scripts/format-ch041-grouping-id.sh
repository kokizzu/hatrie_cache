#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/grouping_sets.go hat/hatSql/ch041_one_pass_grouping.go hat/hatSql/ch041_grouping_id_test.go
