#!/bin/sh
set -eu

printf '%s\n' 'Timestamp references:'
rg -n -i 'timestamp' hat cmd scripts || true
printf '%s\n' 'Epoch references:'
rg -n -i 'epoch' hat cmd scripts || true
printf '%s\n' 'Lease references:'
rg -n -i 'lease' hat cmd scripts || true
