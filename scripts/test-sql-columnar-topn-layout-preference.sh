#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestHatTrieSQLColumnarTopNUsesWarmLayout$' -count=1
