#!/usr/bin/env sh
set -eu

root=$(git rev-parse --show-toplevel)
cd "$root"

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to replace an existing staged change' >&2
	exit 1
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Build an index-only query snapshot from HEAD so concurrent work in the
# working-tree query.go is never included in this feature commit.
git show HEAD:hat/hatSql/query.go > "$tmp/query.go"
awk '
function brace_delta(line, opens, closes) {
	opens = gsub(/\{/, "{", line)
	closes = gsub(/\}/, "}", line)
	return opens - closes
}
{
	if (skip_template) {
		depth += brace_delta($0)
		if (depth == 0) {
			print "\treturn cache.templateWithSchemaVersion(source, \"\")"
			print "}"
			skip_template = 0
		}
		next
	}
	if ($0 == "\tPreparedCache         *SQLPreparedQueryCache") {
		print
		print "\t// PreparedSchemaVersion participates in the prepared-plan cache key. Set it"
		print "\t// when a schema, index, or projection change should force a fresh template."
		print "\tPreparedSchemaVersion string"
		next
	}
	if ($0 == "\tquery, parseErr := parseSQLQueryWithCache(source, parameters, options.PreparedCache)" || $0 == "\tquery, err := parseSQLQueryWithCache(source, parameters, options.PreparedCache)") {
		sub(/options.PreparedCache\)$/, "options.PreparedCache, options.PreparedSchemaVersion)")
		print
		next
	}
	if ($0 == "func parseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache) (*sqlQuery, error) {") {
		print "func parseSQLQueryWithCache(source string, parameters []interface{}, cache *SQLPreparedQueryCache, schemaVersions ...string) (*sqlQuery, error) {"
		next
	}
	if ($0 == "\ttemplate, err := cache.template(source)") {
		print "\tschemaVersion := \"\""
		print "\tif len(schemaVersions) > 0 {"
		print "\t\tschemaVersion = schemaVersions[0]"
		print "\t}"
		print "\ttemplate, err := cache.templateWithSchemaVersion(source, schemaVersion)"
		next
	}
	if ($0 == "func (cache *SQLPreparedQueryCache) template(source string) (*sqlQuery, error) {") {
		print
		print "\treturn cache.templateWithSchemaVersion(source, \"\")"
		print "}"
		skip_template = 1
		depth = brace_delta($0)
		next
	}
	print
}
' "$tmp/query.go" > "$tmp/query.go.new"
mv "$tmp/query.go.new" "$tmp/query.go"

# Keep this command target available for future feature deliveries while
# deriving its staged Makefile from HEAD for the same concurrency guarantee.
git show HEAD:Makefile > "$tmp/Makefile"
awk '
{
	if ($0 == "audit-extensibility-goal:") {
		print "commit-sql-prepared-plan-cache:"
		print "\tsh ./scripts/commit-sql-prepared-plan-cache.sh"
		print ""
		inserted++
	}
	print
}
END {
	if (inserted != 1) {
		exit 1
	}
}
' "$tmp/Makefile" > "$tmp/Makefile.new"
mv "$tmp/Makefile.new" "$tmp/Makefile"

git add -- \
	BENCHMARK.md \
	INSPIRATION.md \
	hat/hatCache/sql_query.go \
	hat/hatSql/keyset.go \
	hat/hatSql/prepared.go \
	hat/hatSql/prepared_cache_benchmark_test.go \
	hat/hatSql/prepared_cache_key.go \
	hat/hatSql/prepared_cache_normalized_test.go \
	scripts/benchmark-sql-prepared-cache.sh \
	scripts/commit-sql-prepared-plan-cache.sh

query_blob=$(git hash-object -w "$tmp/query.go")
git update-index --add --cacheinfo "100644,$query_blob,hat/hatSql/query.go"
makefile_blob=$(git hash-object -w "$tmp/Makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"

git diff --cached --check
git diff --cached --name-only > "$tmp/staged"
printf '%s\n' \
	BENCHMARK.md \
	INSPIRATION.md \
	Makefile \
	hat/hatCache/sql_query.go \
	hat/hatSql/keyset.go \
	hat/hatSql/prepared.go \
	hat/hatSql/prepared_cache_benchmark_test.go \
	hat/hatSql/prepared_cache_key.go \
	hat/hatSql/prepared_cache_normalized_test.go \
	hat/hatSql/query.go \
	scripts/benchmark-sql-prepared-cache.sh \
	scripts/commit-sql-prepared-plan-cache.sh > "$tmp/expected"
diff -u "$tmp/expected" "$tmp/staged"

git commit -m 'perf: normalize version SQL prepared-plan cache'
git push origin master
