#!/bin/sh
set -eu

go test -race ./hat/hatCache -run 'TestTR050ReplicationByteBudget|TestHTTPReplicator' -count=1
