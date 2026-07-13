#!/usr/bin/env sh

if command -v node >/dev/null 2>&1; then
	return 0 2>/dev/null || exit 0
fi

for dir in \
	"$HOME/.nvm/versions/node/v24.18.0/bin" \
	"$HOME/.nvm/versions/node/v22.0.0/bin" \
	"$HOME/.nvm/current/bin"; do
	if [ -x "$dir/node" ]; then
		PATH="$dir:$PATH"
		export PATH
		return 0 2>/dev/null || exit 0
	fi
done

echo "node not found; install Node.js or expose it on PATH" >&2
return 1 2>/dev/null || exit 1
