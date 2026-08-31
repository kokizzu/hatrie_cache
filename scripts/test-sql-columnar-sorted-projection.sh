#!/bin/sh
set -eu

go test ./hat/hatCache -run '^(TestHatTrieSQLColumnarSortedProjectionUsesWarmOrderAndInvalidates|TestHatTrieSQLColumnarCompositeSortedProjectionUsesWarmOrderAndInvalidates|TestHatTrieSQLColumnarDirectedCompositeSortedProjectionUsesWarmOrder)$' -count=1
