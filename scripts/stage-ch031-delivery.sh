#!/usr/bin/env bash
set -euo pipefail

temporary=$(mktemp -d /tmp/hatrie-cache-ch031-delivery-stage.XXXXXX)
cleanup() {
	rm -rf "$temporary"
}
trap cleanup EXIT

git add -- \
	scripts/deliver-ch031-automatic-json-subcolumns.sh \
	scripts/stage-ch031-delivery.sh
git show :Makefile > "$temporary/makefile"
printf '%s\n' \
	'' \
	'.PHONY: commit-ch031-automatic-json-subcolumns' \
	'commit-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/deliver-ch031-automatic-json-subcolumns.sh commit' \
	'.PHONY: push-ch031-automatic-json-subcolumns' \
	'push-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/deliver-ch031-automatic-json-subcolumns.sh push' >> "$temporary/makefile"
makefile_blob=$(git hash-object -w "$temporary/makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --stat
