#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestQuerySubscriptionsStatus' -count=1
