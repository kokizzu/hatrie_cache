#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestSQLTypedInt64CompositeIndex' -count=1
