#!/bin/sh
# Planned failover for ONE shard; block until the old primary is demoted.
# hammerdown exports: HD_KEYSPACE HD_SHARD, HD_FROM_* (old primary), HD_TO_* (new primary)
# Real life (Vitess):
#   vtctldclient PlannedReparentShard "$HD_KEYSPACE/$HD_SHARD" --new-primary "$HD_TO_NAME"
# Real life (Patroni):
#   patronictl switchover --leader "$HD_FROM_NAME" --candidate "$HD_TO_NAME" --force
set -eu
echo "failover $HD_KEYSPACE/$HD_SHARD $HD_FROM_HOST -> $HD_TO_HOST" >&2
sleep 0.2
