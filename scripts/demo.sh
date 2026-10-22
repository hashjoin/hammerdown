#!/usr/bin/env bash
# Paced demo for recording (asciinema rec --idle-time-limit 2 -c scripts/demo.sh docs/demo.cast)
#   1. an 11,000-node fleet with a new build staged
#   2. a full-speed roll hits a bad failover -> fail-fast stops it
#   3. re-run the same command -> it resumes and finishes
# Env: HD=path/to/hammerdown (default bin/hammerdown), PAUSE=seconds between beats (default 2)
set -uo pipefail

HD=${HD:-bin/hammerdown}
PAUSE=${PAUSE:-2}
KEYSPACES='users|teams|messages|threads'

bold=$'\e[1m'; dim=$'\e[2m'; cyan=$'\e[36m'; reset=$'\e[0m'

say()  { printf '\n%s# %s%s\n' "$dim" "$*" "$reset"; sleep "$PAUSE"; }
show() { printf '%s$%s %s%s%s\n' "$cyan" "$reset" "$bold" "$*" "$reset"; sleep 1; }

# keep the story, drop the per-node log firehose
summary() {
  grep -E '^(--------- (Plan|Roll Result)|keyspaces:|elapsed|shards|primary failures|replica failures|aborted|stopped early|Re-run)|FAIL-FAST|ERROR: primary failure' |
    sed -E 's/^[0-9/]+ ([0-9:]+)\.[0-9]+ /\1 /; s/\( dry-run=false \) //; s/ \(simulated cursed shard\)//'
}
builds()  { sed -n '/Keyspace|/,/^$/p'; }

clear
say "hammerdown: put the hammer down, keep the brakes."
say "A simulated fleet: 11,000 nodes, 2,829 shards, 20 keyspaces, all on v19.0.4."
show "hammerdown sim init --force"
"$HD" sim init --force

say "Ship v20.0.1. Every node goes RestartWait."
show "hammerdown sim stage --to v20.0.1"
"$HD" sim stage --to v20.0.1

show "hammerdown report --limit '$KEYSPACES'"
"$HD" report --limit "$KEYSPACES" 2>/dev/null | builds
sleep "$PAUSE"

say "Full speed: 64 shards in flight. Two shards have a replica that won't catch up."
say "--max-primary-failures defaults to 1, so the first bad failover stops everything."
show "hammerdown roll --workers 64 --dry-run=false --yes --sim-cursed 2"
"$HD" roll --workers 64 --dry-run=false --yes --quiet --sim-cursed 2 2>&1 | summary
sleep "$PAUSE"

say "One failure, zero follow-on damage. In-flight shards finished; nothing new started."
say "Fix the problem, then run the exact same command to resume."
show "hammerdown roll --workers 64 --dry-run=false --yes"
"$HD" roll --workers 64 --dry-run=false --yes --quiet 2>&1 | summary
sleep "$PAUSE"

show "hammerdown report --limit '$KEYSPACES'"
"$HD" report --limit "$KEYSPACES" 2>/dev/null | builds

say "The ~0.2% left on v19 are drained nodes: out of service, never touched."
say "github.com/hashjoin/hammerdown"
