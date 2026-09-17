#!/usr/bin/env bash
set -euo pipefail

test -s PERSISTENT_QUERY_LOG.md
test -s SQL_QUERY_LOG.md
rg -n 'M-U25.*Already adopted' PRODUCT_IDEA_GAPS.md
rg -n 'OpenSQLQueryLog|SQLQueryLogOptions|0600|restart|rotation|SampleRate' PERSISTENT_QUERY_LOG.md SQL_QUERY_LOG.md
