#!/bin/sh
# Print the fleet as a JSON array of nodes (see fleet.Node for the fields).
# Real life: query your CMDB / Consul / topo server / cloud API here, e.g.
#   vtctldclient GetTablets --format json | jq '[ .[] | {...} ]'
exec cat "$(dirname "$0")/inventory.json"
