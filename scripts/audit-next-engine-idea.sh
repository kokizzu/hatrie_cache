#!/usr/bin/env bash
set -euo pipefail

printf '===== unchecked inspiration items =====\n'
rg -n '^[-*] \[ \]' INSPIRATION.md | head -n 260 || true
printf '===== candidate rows with current implementation hints =====\n'
rg -n -i 'CH-004|CH-005|CH-006|CH-007|CH-008|CH-009|CH-010|CH-011|CH-012|CH-014|CH-015|CH-016|CH-017|CH-019|CH-022|CH-023|CH-024|CH-025|CH-026|CH-028|CH-029|CH-030|CH-032|CH-037|CH-038|CH-039|CH-041|CH-047|CH-049|CH-050|MZ-002|MZ-005|MZ-007|MZ-010|MZ-017|MZ-024|MZ-026|MZ-028|MZ-038|MZ-044|TT-009|TT-010|TT-018|TT-019|TT-020|TT-024|TT-025|TT-046|TT-050' ENGINE_IDEAS.md
printf '===== matching implementation symbols =====\n'
rg -n -i 'final read|delete bitmap|mutation queue|row ttl|column ttl|projection advisor|async insert|query max.?threads|query complexity|query profiler|array join|aggregate combinator|approx_count|grouping sets|dynamic subcolumn|format parsing|named settings|frontier|arrangement.*key|arrangement.*compaction|spillable|order arrangement|backup manifest|selective.*restore|slab|histogram|analy[sz]e' hat cmd scripts --glob '*.go' --glob '*.sh' | head -n 320 || true
printf '===== planner statistics and index choice =====\n'
rg -n -i 'SQLJSON.*(Stats|Histogram)|histogram|selectivity|estimate|index.*candidate|choose.*index|cost' hat/hatCache --glob '*.go' | head -n 260 || true
sed -n '360,440p' hat/hatCache/sql_query.go
sed -n '1780,2050p' hat/hatCache/sql_query.go
printf '===== packed boolean predicate paths =====\n'
rg -n -i 'BoolColumns|ColumnarBool|boolean.*predicate|predicate.*boolean|bool.*filter|sqlColumnar.*Bool' hat/hatSql --glob '*.go' | head -n 260 || true
