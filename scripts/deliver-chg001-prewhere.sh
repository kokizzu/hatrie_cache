#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  BENCHMARK.md
  BENCHMARK_CHG001_RAW.txt
  Makefile
  hat/hatSql/catalog.go
  hat/hatSql/chg001_prewhere_benchmark_test.go
  hat/hatSql/chg001_prewhere_test.go
  hat/hatSql/columnar_prewhere.go
  hat/hatSql/contracts.go
  hat/hatSql/query.go
  hat/hatSql/session.go
  scripts/benchmark-chg001-prewhere.sh
  scripts/deliver-chg001-prewhere.sh
  scripts/format-chg001-prewhere.sh
  scripts/test-chg001-prewhere.sh
)

new_paths=(
  BENCHMARK_CHG001_RAW.txt
  hat/hatSql/chg001_prewhere_benchmark_test.go
  hat/hatSql/chg001_prewhere_test.go
  hat/hatSql/columnar_prewhere.go
  scripts/benchmark-chg001-prewhere.sh
  scripts/deliver-chg001-prewhere.sh
  scripts/format-chg001-prewhere.sh
  scripts/test-chg001-prewhere.sh
)

expected_paths() {
  printf '%s\n' "${feature_paths[@]}" | sort
}

assert_staged_empty() {
  if [[ -n "$(git diff --cached --name-only)" ]]; then
    printf '%s\n' 'Refusing to stage CHG-001: unrelated changes are already staged.' >&2
    git diff --cached --name-only >&2
    exit 1
  fi
}

assert_expected_staged() {
  local actual
  actual="$(git diff --cached --name-only | sort)"
  if [[ "$actual" != "$(expected_paths)" ]]; then
    printf '%s\n' 'Refusing delivery: staged paths differ from the CHG-001 allowlist.' >&2
    printf '%s\n' 'Expected:' >&2
    expected_paths >&2
    printf '%s\n' 'Actual:' >&2
    printf '%s\n' "$actual" >&2
    exit 1
  fi
}

stage_generated_file() {
  local path="$1"
  local generated="$2"
  local blob
  blob="$(git hash-object -w --path="$path" "$generated")"
  git update-index --add --cacheinfo "100644,$blob,$path"
}

build_existing_files() {
  local tmpdir="$1"

  git show HEAD:BENCHMARK.md > "$tmpdir/BENCHMARK.md"
  cat >> "$tmpdir/BENCHMARK.md" <<'EOF'

<a id="chg001-columnar-prewhere"></a>
## CHG-001 Automatic Columnar `PREWHERE`

Command: `make benchmark-chg001-prewhere`.

Fixture: 20,000 rows, a 1 KiB payload, `WHERE score = 0` (1% matching rows),
and `SELECT id, payload`. Five `-benchmem` samples were run on Linux/amd64
with an AMD Ryzen 9 5950X. `source-bytes/op` is the logical field payload
requested from the resolver, not an estimate of a particular wire codec.

| Path | Median ns/op | Median B/op | Median allocs/op | Source bytes/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing all-column scan | 1,888,818 | 1,416,587 | 1,629 | 20,800,000 | 1.00x |
| Automatic two-phase `PREWHERE` | 1,849,554 | 1,426,478 | 1,643 | 366,400 | 1.02x faster |

The two-phase resolver reduces logical source transfer by 56.8x (98.2%) and
is 2.1% faster in this local fixture. Its cost is 0.7% more transient bytes
and 0.86% more allocations because the projection batch is compacted to
matching row indexes. Sources opt in through `ColumnarPrewhereSourceResolver`;
all existing resolvers retain the legacy path unchanged. Raw samples are in
[`BENCHMARK_CHG001_RAW.txt`](BENCHMARK_CHG001_RAW.txt).
EOF

  git show HEAD:Makefile > "$tmpdir/Makefile"
  cat >> "$tmpdir/Makefile" <<'EOF'

.PHONY: test-chg001-prewhere
test-chg001-prewhere:
	bash ./scripts/test-chg001-prewhere.sh

.PHONY: benchmark-chg001-prewhere
benchmark-chg001-prewhere:
	bash ./scripts/benchmark-chg001-prewhere.sh

.PHONY: format-chg001-prewhere
format-chg001-prewhere:
	bash ./scripts/format-chg001-prewhere.sh

.PHONY: stage-chg001-prewhere commit-chg001-prewhere push-chg001-prewhere deliver-chg001-prewhere status-chg001-prewhere unstage-chg001-prewhere
stage-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh stage
commit-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh commit
push-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh push
deliver-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh deliver
status-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh status
unstage-chg001-prewhere:
	bash ./scripts/deliver-chg001-prewhere.sh unstage
EOF

  git show HEAD:hat/hatSql/catalog.go > "$tmpdir/catalog.go"
  cat >> "$tmpdir/catalog.go" <<'EOF'

// ResolveSQLColumnarPrewhere forwards the optional two-phase scan contract
// while keeping information-schema sources local.
func (resolver CatalogResolver) ResolveSQLColumnarPrewhere(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, false, nil
	}
	prewhere, ok := resolver.Source.(ColumnarPrewhereSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return prewhere.ResolveSQLColumnarPrewhere(name, key, fields)
}

// ResolveSQLColumnarProjection forwards the compact projection phase of the
// optional two-phase scan contract.
func (resolver CatalogResolver) ResolveSQLColumnarProjection(name, key string, fields []string, rowIndexes []int) (ColumnarBatch, bool, error) {
	if catalogOwnsVirtualSource(name, key) || resolver.Source == nil {
		return ColumnarBatch{}, false, nil
	}
	prewhere, ok := resolver.Source.(ColumnarPrewhereSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return prewhere.ResolveSQLColumnarProjection(name, key, fields, rowIndexes)
}
EOF

  git show HEAD:hat/hatSql/contracts.go > "$tmpdir/contracts.go"
  cat >> "$tmpdir/contracts.go" <<'EOF'

// ColumnarPrewhereSourceResolver optionally separates a selective scan into
// predicate and projection phases. The first phase returns only predicate
// fields for every source row. The second phase receives the matching source
// row indexes and may return compact, match-only projection columns. Returning
// available=false from either phase keeps the established columnar resolver
// path unchanged.
type ColumnarPrewhereSourceResolver interface {
	ResolveSQLColumnarPrewhere(name, key string, fields []string) (ColumnarBatch, bool, error)
	ResolveSQLColumnarProjection(name, key string, fields []string, rowIndexes []int) (ColumnarBatch, bool, error)
}
EOF

  git show HEAD:hat/hatSql/session.go > "$tmpdir/session.go"
  cat >> "$tmpdir/session.go" <<'EOF'

// ResolveSQLColumnarPrewhere forwards the optional two-phase scan contract
// after preserving session-local source precedence.
func (session *SQLSession) ResolveSQLColumnarPrewhere(name, key string, fields []string) (ColumnarBatch, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return ColumnarBatch{}, false, nil
	}
	prewhere, ok := session.source.(ColumnarPrewhereSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return prewhere.ResolveSQLColumnarPrewhere(name, key, fields)
}

// ResolveSQLColumnarProjection forwards the compact projection phase of the
// optional two-phase scan contract.
func (session *SQLSession) ResolveSQLColumnarProjection(name, key string, fields []string, rowIndexes []int) (ColumnarBatch, bool, error) {
	if session == nil || session.hasLocalSQLSource(name, key) || session.source == nil {
		return ColumnarBatch{}, false, nil
	}
	prewhere, ok := session.source.(ColumnarPrewhereSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	return prewhere.ResolveSQLColumnarProjection(name, key, fields, rowIndexes)
}
EOF

  git show HEAD:hat/hatSql/query.go > "$tmpdir/query.go.base"
  awk '
    $0 == "\tfields, _, projectionFields, ok := sqlColumnarScanFields(query)" {
      print "\tfields, predicateFields, projectionFields, ok := sqlColumnarScanFields(query)"
      mode = "query"
      next
    }
    $0 == "\tfields, _, projectionFields, ok := sqlColumnarScanFields(q)" {
      print "\tfields, predicateFields, projectionFields, ok := sqlColumnarScanFields(q)"
      mode = "scan"
      next
    }
    mode == "query" && $0 == "\tconditionCache, conditionVersion, conditionCacheable, err := sqlColumnarConditionCacheVersion(query, resolver, control)" {
      print "\tif handled, err := executeSQLColumnarPrewhereQueryRows(query, resolver, columnar, control, visit, predicateFields, projectionFields); handled {"
      print "\t\treturn true, err"
      print "\t}"
      print
      print $0
      mode = ""
      next
    }
    mode == "scan" && $0 == "\tconditionCache, conditionVersion, conditionCacheable, err := sqlColumnarConditionCacheVersion(q, resolver, control)" {
      print "\tif result, handled, err := executeSQLColumnarPrewhereScan(q, resolver, columnar, control, metrics, predicateFields, projectionFields); handled {"
      print "\t\treturn result, true, err"
      print "\t}"
      print
      print $0
      mode = ""
      next
    }
    { print }
  ' "$tmpdir/query.go.base" > "$tmpdir/query.go"

  stage_generated_file BENCHMARK.md "$tmpdir/BENCHMARK.md"
  stage_generated_file Makefile "$tmpdir/Makefile"
  stage_generated_file hat/hatSql/catalog.go "$tmpdir/catalog.go"
  stage_generated_file hat/hatSql/contracts.go "$tmpdir/contracts.go"
  stage_generated_file hat/hatSql/query.go "$tmpdir/query.go"
  stage_generated_file hat/hatSql/session.go "$tmpdir/session.go"
}

stage_feature() {
  assert_staged_empty
  local tmpdir
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' RETURN

  sed -i 's/[[:space:]]*$//' BENCHMARK_CHG001_RAW.txt
  git add -- "${new_paths[@]}"
  build_existing_files "$tmpdir"
  git diff --cached --check
  assert_expected_staged
  git diff --cached --stat
}

commit_feature() {
  assert_expected_staged
  git commit -m 'feat(sql): add automatic columnar prewhere [skip ci]'
}

push_feature() {
  git push origin HEAD
}

unstage_feature() {
  git restore --staged -- "${feature_paths[@]}" 2>/dev/null || true
}

case "${1:-status}" in
  stage) stage_feature ;;
  commit) commit_feature ;;
  push) push_feature ;;
  deliver) stage_feature; commit_feature; push_feature ;;
  status) git status --short; git diff --cached --name-only ;;
  unstage) unstage_feature ;;
  *) printf 'usage: %s {stage|commit|push|deliver|status|unstage}\n' "$0" >&2; exit 2 ;;
esac
