#!/usr/bin/env bash
set -euo pipefail

git add \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  INSPIRATION.md \
  Makefile \
  README.md \
  MG14_SINK_DELIVERY_RETRY_QUEUE.md \
  hat/hatPipeline/m14_sink_retry_queue.go \
  hat/hatPipeline/m14_sink_retry_queue_benchmark_test.go \
  hat/hatPipeline/m14_sink_retry_queue_test.go \
  scripts/mg14-benchmark-baseline.sh \
  scripts/mg14-benchmark.sh \
  scripts/mg14-doc-verify.sh \
  scripts/mg14-followup-commit.sh \
  scripts/mg14-format.sh \
  scripts/mg14-package-test.sh \
  scripts/mg14-race.sh \
  scripts/mg14-commit.sh \
  scripts/mg14-push.sh \
  scripts/mg14-stage.sh \
  scripts/mg14-stage-verify.sh \
  scripts/mg14-test.sh \
  scripts/mg14-vet.sh
