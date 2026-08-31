#!/bin/sh
set -eu

go test ./hat/hatCache -run '^(TestHatTrieSQLColumnarSortedProjectionUsesWarmOrderAndInvalidates|TestHatTrieSQLColumnarCompositeSortedProjectionUsesWarmOrderAndInvalidates)$' -count=1
