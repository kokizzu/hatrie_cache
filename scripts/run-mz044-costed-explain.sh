#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
format)
	gofmt -w hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/tooling.go hat/hatSql/explain_dataflow.go hat/hatSql/explain_pipeline.go hat/hatSql/result_cache.go hat/hatSql/mz044_costed_explain.go hat/hatSql/mz044_costed_explain_test.go hat/hatSql/mz044_costed_explain_benchmark_test.go hat/hatSql/mz044_costed_explain_baseline_benchmark_test.go
	;;
test)
	go test ./hat/hatSql -run '^TestMZ044' -count=1
	;;
benchmark)
	go test ./hat/hatSql -run '^$' -bench '^BenchmarkCostedExplainMZ044' -benchmem -count=5
	;;
baseline)
	go test -tags mz044baseline ./hat/hatSql -run '^$' -bench '^BenchmarkMZ044BaselineRegularExplain$' -benchmem -count=5
	;;
race)
	go test -race ./hat/hatSql -run '^TestMZ044' -count=1
	;;
vet)
	go vet ./hat/hatSql
	;;
package)
	go test ./hat/hatSql -count=1
	;;
race-package)
	go test -race ./hat/hatSql -count=1
	;;
status)
	git status --short --branch
	;;
stage)
  git add Makefile ENGINE_IDEAS.md BENCHMARK.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md MZ044_COSTED_EXPLAIN.md \
		hat/hatSql/model.go hat/hatSql/query.go hat/hatSql/tooling.go \
		hat/hatSql/explain_dataflow.go hat/hatSql/explain_pipeline.go hat/hatSql/result_cache.go \
		hat/hatSql/mz044_costed_explain.go \
		hat/hatSql/mz044_costed_explain_test.go hat/hatSql/mz044_costed_explain_benchmark_test.go \
		hat/hatSql/mz044_costed_explain_baseline_benchmark_test.go \
		scripts/audit-inspiration-gaps.sh scripts/run-mz044-costed-explain.sh
	;;
commit)
	git commit -m 'add costed SQL explain mode [skip ci]'
	;;
push)
	git push origin HEAD:master
	;;
check)
	git diff --check
	;;
docs)
  rg -n 'MZ-044|EXPLAIN COST|costed explain' PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md MZ044_COSTED_EXPLAIN.md ENGINE_IDEAS.md BENCHMARK.md
	;;
*)
	printf '%s\n' 'usage: run-mz044-costed-explain.sh {format|test|baseline|benchmark|race|vet|package|race-package|status|stage|commit|push|check|docs}' >&2
	exit 2
	;;
esac
