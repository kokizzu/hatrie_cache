#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestHatTrieSQLColumnarSortedProjectionUsesWarmOrderAndInvalidates$' -count=1
