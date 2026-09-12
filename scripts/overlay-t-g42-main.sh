#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
	printf 'usage: %s /path/to/hatCache/main.go\n' "$0" >&2
	exit 2
fi

main_file="$1"

if ! grep -q 'keyPrefixWatchers' "$main_file"; then
	temp_file="$main_file.t-g42"
	if ! grep -q 'keyWatchers[[:space:]]' "$main_file"; then
		awk '
			BEGIN { inserted = 0 }
			{
				print
				if ($0 ~ /\tlevelDBHotValues[[:space:]]+map\[string\]int64$/) {
					print "\tkeyWatchers                       map[string]map[uint64]*KeyWatcher"
					print "\tkeyPrefixWatchers                 map[string]map[uint64]*KeyWatcher"
					print "\tnextKeyWatcherID                  uint64"
					inserted++
				}
			}
			END { if (inserted != 1) exit 1 }
		' "$main_file" > "$temp_file"
	else
		awk '
			BEGIN { inserted = 0 }
			{
				print
				if (inserted == 0 && $0 ~ /\tkeyWatchers[[:space:]]+map\[string\]map\[uint64\]\*KeyWatcher$/) {
					print "\tkeyPrefixWatchers                 map[string]map[uint64]*KeyWatcher"
					inserted++
				}
			}
			END { if (inserted != 1) exit 1 }
		' "$main_file" > "$temp_file"
	fi
	mv "$temp_file" "$main_file"
fi

if ! grep -q 'closeKeyWatchersLocked' "$main_file"; then
	temp_file="$main_file.t-g42"
	awk '
		BEGIN { in_destroy = 0; inserted = 0 }
		{
			if ($0 ~ /^func \(ht \*HatTrie\) Destroy\(\)/) {
				in_destroy = 1
			}
			print
			if (in_destroy && $0 ~ /^[[:space:]]*defer ht\.mu\.Unlock\(\)[[:space:]]*$/) {
				print ""
				print "\tht.closeKeyWatchersLocked()"
				in_destroy = 0
				inserted++
			}
		}
		END { if (inserted != 1) exit 1 }
	' "$main_file" > "$temp_file"
	mv "$temp_file" "$main_file"
fi

if ! grep -q 'notifyKeyWatchersLocked(key, KeyChangeSet)' "$main_file"; then
	temp_file="$main_file.t-g42"
	awk '
		BEGIN { after_epoch = 0; inserted = 0 }
		{
			print
			if ($0 ~ /^[[:space:]]*ht\.mutationEpoch\+\+[[:space:]]*$/) {
				after_epoch = 1
				next
			}
			if (after_epoch && $0 ~ /^[[:space:]]*for _, key := range keys \{[[:space:]]*$/) {
				print "\t\tht.notifyKeyWatchersLocked(key, KeyChangeSet)"
				inserted++
			}
			after_epoch = 0
		}
		END { if (inserted != 1) exit 1 }
	' "$main_file" > "$temp_file"
	mv "$temp_file" "$main_file"
fi

if ! grep -q 'notifyKeyWatchersLocked(key, KeyChangeDelete)' "$main_file"; then
	temp_file="$main_file.t-g42"
	awk '
		BEGIN { inserted = 0 }
		{
			print
			if (inserted == 0 && $0 ~ /^[[:space:]]*ht\.recordWriteBatchLocked\(batch\)[[:space:]]*$/) {
				print "\tht.notifyKeyWatchersLocked(key, KeyChangeDelete)"
				inserted++
			}
		}
		END { if (inserted != 1) exit 1 }
	' "$main_file" > "$temp_file"
	mv "$temp_file" "$main_file"
fi

gofmt -w "$main_file"
