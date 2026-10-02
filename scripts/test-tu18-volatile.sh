#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestVolatileHatTrie' -count=1
