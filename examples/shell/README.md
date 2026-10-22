# shell backend example

Six fake MySQL hosts, two shards, scripts that only `echo` and `sleep`.
Swap the bodies of the scripts for real commands and you have a working
control plane for your fleet.

```sh
cd examples/shell
B="--backend shell --shell-inventory ./inventory.sh --shell-restart ./restart.sh --shell-failover ./failover.sh --shell-lock-dir /tmp/hd-locks"

hammerdown report $B
hammerdown roll   $B --workers 2                         # dry run
hammerdown roll   $B --workers 2 --dry-run=false --yes   # runs the scripts
```

Templates work too, if you'd rather not write scripts:

```sh
--shell-restart  'ssh {{.Node.Host}} sudo systemctl restart mysqld'
--shell-failover 'vtctldclient PlannedReparentShard {{.Shard.Keyspace}}/{{.Shard.Name}} --new-primary {{.To.Name}}'
--shell-guard    './no-backup-running.sh'   # non-zero exit skips the shard
```

The inventory is re-read on every invocation, so a node that your restart
script really upgraded will report `OK` next time and drop out of the plan.
That is what makes "re-run the same command to resume" work.
