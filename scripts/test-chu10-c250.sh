#!/usr/bin/env bash
set -euo pipefail

tmp_dir=$(mktemp -d /tmp/hatrie-cache-chu10-test.XXXXXX)
trap 'rm -rf "$tmp_dir"' EXIT

git archive HEAD | tar -x -C "$tmp_dir"
cp hat/hatSql/projection_advisor.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u10_projection_feedback_test.go "$tmp_dir/hat/hatSql/"
cp hat/hatSql/ch_u10_projection_feedback_integration_test.go "$tmp_dir/hat/hatSql/"
(cd "$tmp_dir" && go test ./hat/hatSql -run 'TestSQLProjectionAdvisor(CostRecommendationsUseObservedLatency|RecordsExecutionLatency)$' -count=1)
