#!/bin/sh
set -eu

go test ./hat/hatSql ./hat/hatSql/incremental_window_contract_test
