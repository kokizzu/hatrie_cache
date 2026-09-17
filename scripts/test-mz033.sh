#!/bin/sh
set -eu

go test ./hat/hatSql -run '^(TestSQLDataflowIndexAdvisor|ExampleSQLDataflowIndexAdvisor)$'
