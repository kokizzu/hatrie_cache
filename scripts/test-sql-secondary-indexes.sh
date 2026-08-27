#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLBitmapSecondaryIndexIntersectionAndUnion$'
