#!/bin/sh
# Restart ONE node and block until it is healthy on its target build.
# hammerdown exports: HD_NAME HD_HOST HD_KEYSPACE HD_SHARD HD_CELL HD_ROLE HD_RUNNING HD_TARGET
# Real life:
#   ssh "$HD_HOST" "sudo systemctl restart mysqld" &&
#   until mysql -h "$HD_HOST" -e 'select 1' >/dev/null 2>&1; do sleep 2; done
set -eu
echo "restart $HD_HOST ($HD_KEYSPACE/$HD_SHARD $HD_ROLE) $HD_RUNNING -> $HD_TARGET" >&2
sleep 0.2
