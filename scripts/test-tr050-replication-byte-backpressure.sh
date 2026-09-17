#!/bin/sh
set -eu

go test ./hat/hatCache -run 'TestTR050ReplicationByteBudget' -count=1
