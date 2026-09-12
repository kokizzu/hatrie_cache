#!/bin/sh
set -eu
git add Makefile VISIBILITY_QUEUE.md hat/hatDataStructure/visibility_queue.go hat/hatDataStructure/visibility_queue_test.go scripts/review-visibility-fencing.sh scripts/commit-visibility-fencing.sh scripts/push-visibility-fencing.sh
git commit -m "fix: fence stale visibility queue leases"
