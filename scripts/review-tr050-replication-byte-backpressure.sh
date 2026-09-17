#!/bin/sh
set -eu

git diff --check
git diff --cached --check
git diff -- hat/hatCache/monitoring.go hat/hatCache/replication.go hat/hatCache/replication_test.go hat/hatReplication/model.go
git diff --cached --stat
git diff --cached -- hat/hatCache/monitoring.go hat/hatCache/replication.go hat/hatCache/replication_test.go hat/hatReplication/model.go
git diff --stat
git status --short
