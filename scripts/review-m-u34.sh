#!/usr/bin/env bash
set -euo pipefail

git status --short --branch
git diff --check
git diff --stat
git diff --name-only
git diff -- hat/hatCache/journal_sink.go hat/hatCache/journal_subscription.go
sed -n '1,260p' hat/hatCache/journal_checkpoint.go
sed -n '1,280p' hat/hatCache/m_u34_subscription_checkpoint_test.go
sed -n '1,240p' hat/hatCache/m_u34_subscription_checkpoint_benchmark_test.go
