#!/usr/bin/env bash
# Injects faults into the Docker Compose cluster while kvcheck runs, then
# fails unless kvcheck finds the recorded history linearizable and every
# replica converged. Start the cluster first (make cluster).
#
# Faults, in order:
#   1. partition  node1 is disconnected from the network for 15s
#   2. crash      node2 is killed with SIGKILL and restarted from its WAL
#   3. pause      node3 is frozen for 8s, like a long GC pause
#   4. full crash all three nodes are killed and restarted
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -f deploy/docker-compose.yml)
project=kvstore
network="${project}_default"
container() { echo "${project}-$1-1"; }
log() { printf '%s  %s\n' "$(date +%T)" "$*"; }
status() { "${compose[@]}" run --rm tools -addr "$1:7000" status || true; }

docker rm -f kvcheck >/dev/null 2>&1 || true
trap 'docker rm -f kvcheck >/dev/null 2>&1 || true' EXIT

log "starting kvcheck: 16 clients on 8 keys for 80s"
"${compose[@]}" run -d --name kvcheck --entrypoint /usr/local/bin/kvcheck tools \
  -addrs node1:7000,node2:7000,node3:7000 -clients 16 -keys 8 -duration 80s >/dev/null

sleep 10
log "1. partition: disconnecting node1"
status node1
docker network disconnect "$network" "$(container node1)"
sleep 15
# Reconnecting does not restore the Compose service alias, so add it back.
docker network connect --alias node1 "$network" "$(container node1)"
log "   node1 reconnected"

sleep 10
log "2. crash: SIGKILL node2, restart in 5s"
docker kill --signal KILL "$(container node2)" >/dev/null
sleep 5
docker start "$(container node2)" >/dev/null

sleep 10
log "3. pause: freezing node3 for 8s"
docker pause "$(container node3)" >/dev/null
sleep 8
docker unpause "$(container node3)" >/dev/null

sleep 10
log "4. full crash: SIGKILL every node, restart in 3s"
docker kill --signal KILL "$(container node1)" "$(container node2)" "$(container node3)" >/dev/null
sleep 3
docker start "$(container node1)" "$(container node2)" "$(container node3)" >/dev/null

log "waiting for kvcheck to finish its run, final reads, check and convergence wait"
code=$(docker wait kvcheck)
docker logs kvcheck
for n in node1 node2 node3; do status "$n"; done
if [ "$code" != "0" ]; then
  log "FAIL: kvcheck exited with status $code"
  exit 1
fi
log "PASS"
