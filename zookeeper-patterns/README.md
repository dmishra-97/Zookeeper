# ZooKeeper Patterns — Spring Boot

A hands-on learning project demonstrating six core ZooKeeper coordination patterns, each exposed as a REST API so you can observe the behavior interactively.

| Pattern | What it solves | Real-world example |
|---|---|---|
| [Leader Election](#1-leader-election) | In a multi-node cluster, only one node should perform privileged work (scheduling, rebalancing, writing to an authoritative store). Ephemeral-sequential ZNodes give every node a queue number; the lowest wins. When the leader crashes its ZNode disappears and the next-lowest takes over — no election broadcast, no manual intervention. | **Kafka controller** — one broker is elected controller and owns partition-leader assignment. When it goes down, ZooKeeper triggers re-election and a new controller is live within seconds. |
| [Distributed Lock](#2-distributed-lock) | Multiple processes racing to mutate shared state need serialization without a central database bottleneck. ZooKeeper provides three lock flavors: exclusive mutex (one holder at a time), read-write (concurrent reads, exclusive write), and semaphore (bounded concurrency). Waiters watch only their predecessor node, preventing thundering-herd on release. | **Payment processing** — a charge request arrives at three service replicas simultaneously. A ZK mutex ensures exactly one replica executes the charge; the others queue and retry, eliminating double-charges without a database-level row lock. |
| [Service Discovery](#3-service-registry--discovery) | Hardcoding service addresses breaks the moment a pod restarts or scales. Each instance registers an ephemeral ZNode with its host/port on startup. ZooKeeper deletes the node automatically on crash or disconnect. Clients watch the parent path and receive push notifications — no polling, no stale entries. | **Microservice mesh** — an `orders` service starts three replicas, each registering itself. The API gateway discovers live instances via ZooKeeper and load-balances across them. If replica-2 OOM-crashes, its ephemeral node vanishes within the session timeout and the gateway stops routing to it. |
| [Config Management](#4-config-management) | Restarting a fleet of services to change a flag or threshold is slow and risky. Config values live as persistent ZNodes; every service holds a `CuratorCache` watcher on its keys. A single write propagates to all watchers in milliseconds — no restart, no polling interval lag. | **Feature flags** — a `feature.new-checkout` flag is `false` in ZooKeeper. Product flips it to `true` via an internal dashboard. Within ~50 ms all 40 running checkout service instances receive the watcher callback, enable the new flow, and log the change — zero deployments. |
| [Distributed Barrier](#5-distributed-barrier) | Parallel workers in a pipeline must not start the next phase until all workers have finished the current one. A single ZNode acts as a gate: workers block until the coordinator removes it (simple barrier), or all workers rendezvous at both the enter and leave points before any proceeds (double barrier). | **ETL batch job** — 20 Spark map tasks run in parallel. A ZK double barrier ensures no reduce task starts until every map task has written its output partition. Once all 20 workers call `leave`, the barrier lifts and all reduce tasks start together. |
| [Distributed Counter](#6-distributed-counter) | A counter that must be accurate across multiple JVMs cannot use an in-memory `AtomicLong` — that's local only. ZooKeeper's `DistributedAtomicLong` uses optimistic compare-and-set (CAS) with automatic retry; `SharedCount` adds watcher-based push to all observers on every change. | **Global rate limiter** — an API gateway runs on 10 nodes, each handling requests independently. All 10 increment the same ZK atomic counter per second. The shared value enforces a hard global cap (e.g., 1 000 req/s) instead of per-node caps that would allow 10 000 req/s in aggregate. |

---

## Prerequisites

| Tool | Version | Check |
|---|---|---|
| Java | 17+ | `java -version` |
| Maven | 3.8+ | `mvn -version` |
| Docker + Compose | any | `docker compose version` |

---

## Setup

### 1. Clone and enter the project

```bash
cd /Users/divyanshu/Desktop/Projects/Zookeeper
```

### 2. Start ZooKeeper

```bash
docker-compose up -d
```

This starts:
- **ZooKeeper** on `localhost:2181`
- **ZooNavigator** UI at `http://localhost:9000` — visual ZNode tree browser. Connect string: `zookeeper:2181` (use the Docker service name, not `localhost` — ZooNavigator runs inside Docker and can't reach `localhost:2181`)

Verify ZooKeeper is ready:

```bash
docker logs zookeeper 2>&1 | grep "binding to port"
# Expected: binding to port 0.0.0.0/0.0.0.0:2181
```

### 3. Start the application

```bash
mvn spring-boot:run
```

App starts on `http://localhost:8080`. Watch the logs — you'll see ZooKeeper connections and the leader election firing immediately.

### 4. Run the automated test suite (no Docker needed)

Tests use Curator's embedded `TestingServer`, so ZooKeeper does not need to be running.

```bash
mvn test
```

Expected output:
```
Tests run: 11, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

---

## Scenario Walkthroughs

> All `curl` commands assume the app is running on `http://localhost:8080`.  
> Pipe to `| jq` for pretty JSON (install with `brew install jq`).

---

### 1. Leader Election

**Core idea:** Each node registers an ephemeral-sequential ZNode. The node with the lowest sequence number is the leader. When it disconnects, the next-lowest takes over — no broadcast, no vote.

#### Scenario A — Single node wins immediately

```bash
# Check if this instance is the leader
curl -s http://localhost:8080/api/leader-election/status | jq
```

Expected (single instance running):
```json
{
  "nodeId": "a3f2c1b4",
  "isLeader": true,
  "description": "Start multiple app instances on different ports to see election in action"
}
```

#### Scenario B — Simulate failover with resign

```bash
# 1. Confirm leadership
curl -s http://localhost:8080/api/leader-election/status | jq

# 2. Force this node to resign (simulates a crash-and-restart)
curl -s -X POST http://localhost:8080/api/leader-election/resign | jq

# 3. Check status again — node re-enters election and wins again (only one node)
curl -s http://localhost:8080/api/leader-election/status | jq
```

#### Scenario C — Multi-instance election (two terminals)

```bash
# Terminal 1 — start first instance (default port 8080)
mvn spring-boot:run

# Terminal 2 — start second instance on a different port
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8081"

# Check which one is leader
curl -s http://localhost:8080/api/leader-election/status | jq   # likely leader
curl -s http://localhost:8081/api/leader-election/status | jq   # likely follower

# Resign on the leader — watch the follower become leader (check logs)
curl -s -X POST http://localhost:8080/api/leader-election/resign | jq
curl -s http://localhost:8081/api/leader-election/status | jq   # now isLeader: true
```

---

### 2. Distributed Lock

**Core idea:** Ephemeral-sequential ZNodes under a lock path. The lowest sequence holds the lock; others watch the node just before them (prevents thundering herd).

#### Scenario A — Exclusive mutex (serial execution)

Open two terminals and fire both commands at the same time:

```bash
# Terminal 1
curl -s -X POST "http://localhost:8080/api/distributed-lock/mutex?workMs=3000" | jq

# Terminal 2 (immediately after Terminal 1)
curl -s -X POST "http://localhost:8080/api/distributed-lock/mutex?workMs=3000" | jq
```

What to observe:
- Terminal 1 completes after ~3 seconds.
- Terminal 2 completes after ~6 seconds (queued, not rejected).
- App logs show `Lock acquired` and `Lock released` interleaved.

#### Scenario B — Read-Write lock (concurrent reads blocked by write)

```bash
# Fire 3 read requests simultaneously — all 3 complete in ~1s (concurrent)
curl -s -X POST "http://localhost:8080/api/distributed-lock/read?workMs=1000" | jq &
curl -s -X POST "http://localhost:8080/api/distributed-lock/read?workMs=1000" | jq &
curl -s -X POST "http://localhost:8080/api/distributed-lock/read?workMs=1000" | jq &
wait

# Now mix a write — it must wait for all readers to finish
curl -s -X POST "http://localhost:8080/api/distributed-lock/write?workMs=2000" | jq &
curl -s -X POST "http://localhost:8080/api/distributed-lock/read?workMs=1000" | jq &
wait
```

What to observe in logs:
- All read locks are acquired in parallel.
- The write lock waits until all read locks are released.

#### Scenario C — Semaphore (bounded concurrency, max 3)

```bash
# Fire 4 concurrent requests — the 4th is rejected (all 3 slots busy)
for i in 1 2 3 4; do
  curl -s -X POST "http://localhost:8080/api/distributed-lock/semaphore?workMs=3000" | jq &
done
wait
```

What to observe:
- Three requests return success after ~3 seconds.
- The fourth returns immediately: `"Could not acquire semaphore lease — all 3 slots are busy"`.

---

### 3. Service Registry / Discovery

**Core idea:** Services register by creating an **ephemeral** ZNode. If the process crashes, ZooKeeper deletes the ZNode automatically — instant deregistration. Clients watch via `PathChildrenCache` and get push notifications, no polling.

#### Scenario A — Register and discover

```bash
# Register three instances of an "orders" service
curl -s -X POST "http://localhost:8080/api/service-discovery/register?name=orders&host=10.0.0.1&port=8081" | jq
curl -s -X POST "http://localhost:8080/api/service-discovery/register?name=orders&host=10.0.0.2&port=8082" | jq
curl -s -X POST "http://localhost:8080/api/service-discovery/register?name=orders&host=10.0.0.3&port=8083" | jq

# Discover all live instances
curl -s http://localhost:8080/api/service-discovery/discover/orders | jq

# Pick one at random (simulates client-side load balancing)
curl -s http://localhost:8080/api/service-discovery/discover/orders/one | jq

# List all registered service names
curl -s http://localhost:8080/api/service-discovery/services | jq
```

#### Scenario B — Graceful deregistration (shutdown)

```bash
# Save the instanceId from a registration
INSTANCE_ID=$(curl -s -X POST "http://localhost:8080/api/service-discovery/register?name=payments&host=10.0.1.1&port=9001" | jq -r '.id')

# Discover — 1 instance
curl -s "http://localhost:8080/api/service-discovery/discover/payments" | jq

# Deregister (simulates graceful shutdown)
curl -s -X DELETE "http://localhost:8080/api/service-discovery/deregister/$INSTANCE_ID" | jq

# Discover again — 0 instances
curl -s "http://localhost:8080/api/service-discovery/discover/payments" | jq
```

#### Scenario C — Crash simulation (session expiry)

ZooKeeper deletes ephemeral nodes when the session expires. To simulate this without code:

```bash
# Register a service
curl -s -X POST "http://localhost:8080/api/service-discovery/register?name=inventory&host=10.0.2.1&port=7001" | jq

# Stop ZooKeeper — ephemeral nodes will be gone when it restarts
docker-compose stop zookeeper
sleep 5
docker-compose start zookeeper

# After reconnect, discover — the inventory service should be gone
curl -s "http://localhost:8080/api/service-discovery/discover/inventory" | jq
```

---

### 4. Config Management

**Core idea:** Config values live as persistent ZNodes. Each service watches its keys with `CuratorCache`. When a value is written, ZooKeeper pushes the change to all watchers within milliseconds — no restart, no polling.

#### Scenario A — Read the pre-seeded config

```bash
# All config keys (seeded on startup)
curl -s http://localhost:8080/api/config | jq

# Single key
curl -s "http://localhost:8080/api/config/feature.dark-mode" | jq
curl -s "http://localhost:8080/api/config/rate-limit.requests-per-second" | jq
```

#### Scenario B — Live update via watcher

```bash
# Terminal 1 — tail the app logs so you can see the watcher fire
# (or watch the logs wherever spring-boot:run is running)

# Terminal 2 — update a config value
curl -s -X PUT "http://localhost:8080/api/config/feature.dark-mode?value=true" | jq
```

What to observe in logs:
```
[Config] Live update: feature.dark-mode changed false -> true
```

```bash
# Verify the local cache was updated
curl -s "http://localhost:8080/api/config/feature.dark-mode" | jq
# { "key": "feature.dark-mode", "value": "true" }
```

#### Scenario C — Add and delete a custom key

```bash
# Create a new config key
curl -s -X PUT "http://localhost:8080/api/config/circuit-breaker.threshold?value=50" | jq

# Confirm it exists
curl -s http://localhost:8080/api/config | jq

# Delete it
curl -s -X DELETE "http://localhost:8080/api/config/circuit-breaker.threshold" | jq

# Confirm removal
curl -s http://localhost:8080/api/config | jq
```

#### Scenario D — Observe from ZooNavigator

1. Open `http://localhost:9000` and connect to `zookeeper:2181`
2. Navigate to `/patterns/config`
3. Update a key's data directly in the UI
4. Watch the app logs — the watcher fires and updates the local cache instantly

---

### 5. Distributed Barrier

**Core idea:** A gate (ZNode) that blocks workers until a coordinator removes it. All blocked workers are released simultaneously — useful for synchronizing parallel batch jobs.

#### Scenario A — Single-phase gate (all workers released at once)

Open three terminals:

```bash
# Terminal 1 — coordinator raises the gate
curl -s -X POST http://localhost:8080/api/barrier/set | jq

# Terminal 2 — worker 1 starts waiting (this call will BLOCK)
curl -s -X POST "http://localhost:8080/api/barrier/wait?timeout=30" | jq

# Terminal 3 — worker 2 starts waiting (this call will BLOCK)
curl -s -X POST "http://localhost:8080/api/barrier/wait?timeout=30" | jq

# Back to Terminal 1 — coordinator removes the gate
curl -s -X POST http://localhost:8080/api/barrier/remove | jq
```

What to observe:
- Terminals 2 and 3 unblock at the same time and both return `"lifted": true`.

#### Scenario B — Double barrier (enter + leave rendezvous)

The double barrier ensures all N workers both start and stop together.

```bash
# Fire 3 workers simultaneously (members=3, all must arrive before any proceeds)
curl -s -X POST "http://localhost:8080/api/barrier/double?members=3&workMs=2000" | jq &
curl -s -X POST "http://localhost:8080/api/barrier/double?members=3&workMs=2000" | jq &
curl -s -X POST "http://localhost:8080/api/barrier/double?members=3&workMs=2000" | jq &
wait
```

Follow the logs — you'll see the lifecycle:
```
[DoubleBarrier] barrier-worker-X entering — waiting for 3 members
[DoubleBarrier] barrier-worker-Y entering — waiting for 3 members
[DoubleBarrier] barrier-worker-Z entering — waiting for 3 members
[DoubleBarrier] barrier-worker-X entered — all 3 members present, doing work
[DoubleBarrier] barrier-worker-Y entered — all 3 members present, doing work
[DoubleBarrier] barrier-worker-Z entered — all 3 members present, doing work
[DoubleBarrier] barrier-worker-X leaving — waiting for all members to finish
[DoubleBarrier] barrier-worker-Y leaving — waiting for all members to finish
[DoubleBarrier] barrier-worker-Z leaving — waiting for all members to finish
[DoubleBarrier] barrier-worker-X left — all done
```

Try firing only 2 workers (`members=3`) — they will hang waiting for the third.

---

### 6. Distributed Counter

**Core idea:** Two implementations — `DistributedAtomicLong` (CAS with automatic retry) and `SharedCount` (manual compare-and-set with watchers).

#### Scenario A — Atomic counter basics

```bash
# Get current value
curl -s http://localhost:8080/api/counter/atomic | jq

# Increment
curl -s -X POST http://localhost:8080/api/counter/atomic/increment | jq

# Decrement
curl -s -X POST http://localhost:8080/api/counter/atomic/decrement | jq

# Add a large delta
curl -s -X POST "http://localhost:8080/api/counter/atomic/add?delta=100" | jq

# Reset to zero
curl -s -X POST http://localhost:8080/api/counter/atomic/reset | jq
```

#### Scenario B — Race condition proof (concurrent increments)

```bash
# Reset first
curl -s -X POST http://localhost:8080/api/counter/atomic/reset | jq

# Fire 10 concurrent increments
for i in $(seq 1 10); do
  curl -s -X POST http://localhost:8080/api/counter/atomic/increment | jq '.value' &
done
wait

# Final value must be exactly 10
curl -s http://localhost:8080/api/counter/atomic | jq
```

Without distributed locking, the final value would be less than 10 due to race conditions. ZooKeeper's CAS guarantees it's always exactly 10.

#### Scenario C — SharedCount with compare-and-set

```bash
# Set initial value
curl -s -X PUT "http://localhost:8080/api/counter/shared?value=0" | jq

# Increment (uses CAS — returns 409 if it loses a race)
curl -s -X POST http://localhost:8080/api/counter/shared/increment | jq

# Read current value
curl -s http://localhost:8080/api/counter/shared | jq
```

To observe a CAS conflict, fire two concurrent increments from separate terminals:

```bash
# Both terminals simultaneously
curl -s -X POST http://localhost:8080/api/counter/shared/increment | jq
```

One will return `"success": true`, the other may return HTTP 409 with `"CAS failed — another node incremented first. Retry."` under contention.

---

## Inspect ZNodes in ZooNavigator

Open `http://localhost:9000` → Connect string: `zookeeper:2181`

Navigate the tree to see exactly what ZooKeeper stores for each pattern:

| Path | What you'll find |
|---|---|
| `/patterns/leader-election` | Ephemeral-sequential nodes (`_c_...-latch-0000000000`) |
| `/patterns/distributed-lock/mutex` | Lock nodes appear during an active lock, disappear after release |
| `/patterns/services/{name}/{id}` | Ephemeral nodes with JSON payload (host, port, status) |
| `/patterns/config/{key}` | Persistent nodes — click to view and edit the data |
| `/patterns/barrier/gate` | Appears when barrier is set, disappears on remove |
| `/patterns/counter/atomic` | Stores the current counter value as binary |

---

## Tear down

```bash
# Stop the app: Ctrl+C in the spring-boot:run terminal

# Stop and remove ZooKeeper containers and volumes
docker-compose down -v
```
