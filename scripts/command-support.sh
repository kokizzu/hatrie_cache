#!/usr/bin/env sh
set -eu

source_file=${1:-command.go}

if [ ! -f "$source_file" ]; then
	echo "command source not found: $source_file" >&2
	exit 2
fi

awk '
	/^func \(ht \*HatTrie\) ExecuteCommand/ {
		in_execute = 1
	}
	/^func \(ht \*HatTrie\) executeExactFastCommand/ {
		in_execute = 0
	}
	in_execute && /^[[:space:]]*case "/ {
		line = $0
		sub(/^[[:space:]]*case[[:space:]]*/, "", line)
		sub(/:[[:space:]]*$/, "", line)
		gsub(/"/, "", line)
		gsub(/[[:space:]]*,[[:space:]]*/, ",", line)
		count = split(line, commands, ",")
		aliases = "-"
		if (count > 1) {
			aliases = ""
			for (i = 2; i <= count; i++) {
				if (aliases != "") {
					aliases = aliases ", "
				}
				aliases = aliases "`" commands[i] "`"
			}
		}
		rows[++row_count] = "| `" commands[1] "` | " aliases " |"
	}
	END {
		print "| Canonical command | Accepted aliases |"
		print "| --- | --- |"
		for (i = 1; i <= row_count; i++) {
			print rows[i]
		}
	}
' "$source_file"
