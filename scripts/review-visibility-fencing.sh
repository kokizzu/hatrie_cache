#!/bin/sh
set -eu
git diff --check
git status --short
git diff --stat -- Makefile VISIBILITY_QUEUE.md hat/hatDataStructure/visibility_queue.go hat/hatDataStructure/visibility_queue_test.go
