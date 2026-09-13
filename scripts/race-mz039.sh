#!/bin/sh
set -eu

go test -race ./hat/hatSql -run 'MZ039|SQLNativeDataflow|SQLQuery'
