#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestProjectionCatalog' -count=1
