#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'TestQuerySubscriptionsStatus' -count=1
