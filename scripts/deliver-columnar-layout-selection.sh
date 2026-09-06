#!/usr/bin/env bash
set -euo pipefail

mode=${1:-preview}
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-columnar-layout-delivery.XXXXXX")
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"

if [[ "$mode" == "push" ]]; then
	git push
	git rev-parse HEAD
	exit 0
fi

if [[ "$mode" != "preview" && "$mode" != "commit" ]]; then
	echo "usage: $0 [preview|commit|push]" >&2
	exit 2
fi

target_block=$(cat <<'EOF'
# columnar-layout-selection-targets
format-columnar-layout-selection:
	bash ./scripts/format-columnar-layout-selection.sh

test-columnar-layout-selection:
	bash ./scripts/test-columnar-layout-selection.sh

test-race-columnar-layout-selection:
	bash ./scripts/test-race-columnar-layout-selection.sh

benchmark-columnar-layout-selection:
	bash ./scripts/benchmark-columnar-layout-selection.sh

deliver-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh preview

commit-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh commit

push-columnar-layout-selection:
	bash ./scripts/deliver-columnar-layout-selection.sh push
EOF
)
base_makefile="$tmpdir/Makefile"
git show HEAD:Makefile > "$base_makefile"
if ! grep -Fq -- '# columnar-layout-selection-targets' "$base_makefile"; then
	printf '\n%s\n' "$target_block" >> "$base_makefile"
fi

base_inspiration="$tmpdir/INSPIRATION.md"
git show HEAD:INSPIRATION.md > "$base_inspiration"
child='- [x] C030a Width-aware adaptive dictionary selection for compact columnar layouts.'
if ! grep -Fq -- "$child" "$base_inspiration"; then
	printf '\n%s\n' "$child" >> "$base_inspiration"
fi

GIT_INDEX_FILE="$index" git read-tree HEAD
makefile_blob=$(git hash-object -w "$base_makefile")
inspiration_blob=$(git hash-object -w "$base_inspiration")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"

base_contracts="$tmpdir/contracts.go"
target_contracts="$tmpdir/contracts.target.go"
git show HEAD:hat/hatSql/contracts.go > "$base_contracts"
replacement="$tmpdir/columnar-layout-replacement.txt"
cat > "$replacement" <<'EOF'
// EncodeRepeatedStrings replaces all-string columns with a dictionary when the
// estimated retained layout is smaller. The estimate accounts for row count,
// string width, dictionary codes, and unique string headers.
func (batch *ColumnarBatch) EncodeRepeatedStrings() {
	if batch == nil || batch.Rows < 4 || batch.Columns == nil {
		return
	}
	if batch.Dictionaries == nil {
		batch.Dictionaries = make(map[string]DictionaryColumn)
	}
	for field, values := range batch.Columns {
		if len(values) != batch.Rows {
			continue
		}
		positions := make(map[string]uint32)
		strings := make([]string, 0)
		codes := make([]uint32, len(values))
		totalStringBytes := 0
		uniqueStringBytes := 0
		allStrings := true
		for index, value := range values {
			text, ok := value.(string)
			if !ok {
				allStrings = false
				break
			}
			code, found := positions[text]
			if !found {
				code = uint32(len(strings))
				positions[text] = code
				strings = append(strings, text)
				uniqueStringBytes += len(text)
			}
			totalStringBytes += len(text)
			codes[index] = code
		}
		if !allStrings || !columnarDictionaryLayoutSmaller(len(values), len(strings), totalStringBytes, uniqueStringBytes) {
			continue
		}
		batch.Dictionaries[field] = DictionaryColumn{Values: strings, Codes: codes}
		delete(batch.Columns, field)
	}
}

func columnarDictionaryLayoutSmaller(rows, unique, totalStringBytes, uniqueStringBytes int) bool {
	if rows < 4 || unique == 0 || uniqueStringBytes < 0 || totalStringBytes < uniqueStringBytes {
		return false
	}
	// Keep the dictionary's fixed code and string-header storage lower than the
	// plain interface slice even when repeated strings share backing bytes.
	if uint64(unique)*4 > uint64(rows)*3 {
		return false
	}
	plainBytes := uint64(rows)*16 + uint64(totalStringBytes)
	dictionaryBytes := uint64(rows)*4 + uint64(unique)*16 + uint64(uniqueStringBytes)
	return dictionaryBytes < plainBytes
}
EOF
awk -v replacement="$replacement" '
    /^\/\/ EncodeRepeatedStrings replaces / {
        while ((getline line < replacement) > 0) print line
        close(replacement)
        skipping = 1
        next
    }
    skipping && /^\/\/ ColumnarSourceResolver / {
        skipping = 0
        print ""
    }
    skipping { next }
    { print }
' "$base_contracts" > "$target_contracts"
if ! grep -Fq -- 'columnarDictionaryLayoutSmaller' "$target_contracts"; then
	echo 'failed to build the columnar layout source' >&2
	exit 1
fi
contracts_blob=$(git hash-object -w "$target_contracts")
GIT_INDEX_FILE="$index" git update-index --add --cacheinfo "100644,$contracts_blob,hat/hatSql/contracts.go"

GIT_INDEX_FILE="$index" git add -- \
	COLUMNAR_LAYOUT_SELECTION.md \
	hat/hatSql/columnar_layout_selection_test.go \
	scripts/benchmark-columnar-layout-selection.sh \
	scripts/deliver-columnar-layout-selection.sh \
	scripts/format-columnar-layout-selection.sh \
	scripts/test-columnar-layout-selection.sh \
	scripts/test-race-columnar-layout-selection.sh

GIT_INDEX_FILE="$index" git diff --cached --check
if GIT_INDEX_FILE="$index" git diff --cached --quiet; then
	echo "columnar layout selection already delivered"
	git rev-parse HEAD
	exit 0
fi

echo "columnar layout selection delivery ($mode)"
GIT_INDEX_FILE="$index" git diff --cached --name-status
GIT_INDEX_FILE="$index" git diff --cached --stat

if [[ "$mode" == "commit" ]]; then
	GIT_INDEX_FILE="$index" git commit -m "feat(sql): select compact columnar layouts by width"
	git rev-parse HEAD
fi
