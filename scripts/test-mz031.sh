#!/bin/sh
set -eu

go test ./hat/hatSql -run '^TestC213IncrementalTopKRankChanges'
