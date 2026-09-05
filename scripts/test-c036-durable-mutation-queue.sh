#!/bin/sh
set -eu

go test ./hat/hatCache -run 'Outbox' -count=1
