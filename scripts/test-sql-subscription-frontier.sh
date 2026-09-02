#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestQuerySubscriptionFrontier' -count=1
