#!/bin/sh
set -eu

rg -n 'index\.raw (==|!=) data|sqlJSONIndexSnapshotLocked\(key, data\)|refreshSQLJSON[A-Za-z]+IndexRows\(.*data|sqlJSONSourceString\(key\)' hat/hatCache/sql_query.go
