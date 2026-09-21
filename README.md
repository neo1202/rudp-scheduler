# rudp-scheduler

[![ci](https://github.com/neo1202/rudp-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/neo1202/rudp-scheduler/actions/workflows/ci.yml)

A fault-tolerant distributed job scheduler that runs on its own reliable-UDP
transport. A client submits a job that can be cut into independent ranges; the
server cuts it into chunks; a pool of workers that may join, slow down or
disappear at any moment pull chunks at their own pace; the server folds the
partial results into one answer. All of it runs over UDP that drops,
duplicates and reorders packets, and the design is built around one rule:
**packet loss is handled entirely below a line, slow or dead nodes entirely
above it, and the two layers never learn about each other.** It is written in
Go, has no dependencies outside the standard library, and contains no mutex.

## Architecture

```
   client xN                        server x1                         worker xM
  ------------    ---------------------------------------------     ------------
   Request ->      SCHEDULING LAYER   task, worker, straggler         <- Join
   <- Result       never sees: seq, Ack, retransmission               <- ChunkResult
                                                                      Chunk ->
                     clientHandler ---- nq ----+
                      (1 per request)          |  pull: pq first, then nq,
                                               v  only while inflight < window
                     dispatcher ------> workerLoop (1 per worker) --+
                      (the only caller     owns inflight, scans     |  pq <- clone of a straggler,
                       of srv.Read())      it for stragglers  <-----+  or tasks of a dead worker
                          ^                    |
                          |                    | resultsCh
                          |                    v
                          |              aggregator   owns done + jobs:
                          |               de-dup by TaskID, Merge, reply
                          |                    |
   = = = = = = = = = = = =|= = = THE LINE = = =|= = = = = = = = = = = = = = = = = = =
                          |                    |
                     srv.Read()           h.Write()
                          |                    |
                   TRANSPORT LAYER   seq, Ack, window, RTO, epoch
                   never sees: task, job, payload contents

                     mainLoop (1 per connection)  owns sendBuffer,
                          ^      |                pendingWrites, deliverQ
                          | inCh |
                     readLoop    |  (1 per server: one UDP port, one reader,
                          ^      v   routes by connection ID)
                   ------------------------
                         UDP socket            loses, duplicates, reorders
```

Five kinds of goroutine, each the sole owner of its state:

| Goroutine | Count | Owns |
|---|---|---|
| `readLoop` | 1 per process | the connection table |
| `mainLoop` | 1 per connection | that connection's send buffer, write queue, delivery queue, timers |
| `dispatcher` | 1 | the worker and client tables |
| `workerLoop` | 1 per worker | that worker's in-flight tasks and task-level SRTT |
| `aggregator` | 1 | `done` (which tasks are counted) and `jobs` (running answers) |

`clientHandler` (one per request) registers the job, pushes its tasks into
`nq`, and exits. Clients and workers link the same transport and run one
`readLoop` plus one `mainLoop` each.

The full write-up is in [docs/architecture.md](docs/architecture.md); the byte
layouts are in [docs/wire-format.md](docs/wire-format.md).

## Why hash search, and why out-of-order delivery is correct

The default workload is a proof-of-work style search: find the nonce in
`[lo, hi)` that minimises `SHA-256(msg, nonce)`. It was picked because it
states the scheduler's premise with nothing else in the way:

- it is CPU bound and its chunks share nothing, so it splits as far as you like;
- its merge function is `min`, which is **commutative, associative and idempotent**.

From those two facts the transport design follows. If partial results can be
merged in any order, the transport has no reason to deliver them in order. If
merging the same result twice changes nothing, and the server counts each
`TaskID` once anyway, the transport has no reason to suppress duplicates, and
the scheduler is free to compute a chunk twice on purpose. So `rudp` hands
every message to the application the moment it arrives, acknowledges
duplicates without filtering them, and keeps no reorder buffer. At-least-once,
unordered delivery below plus idempotent, order-free aggregation above gives
exactly-once accounting end to end, and one lost packet never stalls the
packets behind it.

The same reasoning covers any job with that shape: a parameter sweep that
keeps the best score, a fuzzing campaign that keeps the smallest failing
input, a renderer whose tiles are pure functions of their coordinates. A
workload is three functions (`Compute`, `Merge`, `Zero`, see
[workload/workload.go](workload/workload.go)), and the aggregator sees nothing
else. The three algebraic properties of `Merge` are checked with
`testing/quick`, because everything above depends on them.

## Design highlights

- **Pull-based scheduling.** Nobody decides which worker gets a chunk. A
  worker's server-side loop takes a task whenever it has fewer than `window`
  in flight. Fast workers come back sooner, slow ones stop asking. There is no
  speed estimate to keep fresh and nothing to rebalance when a worker joins,
  is throttled, or loses its CPU to a neighbour.
- **Hedging without cancellation.** Each worker loop scans its own in-flight
  tasks every 10 ms; one that is older than `max(3 x SRTT, 50 ms)` is cloned,
  once, into a priority queue that every worker pulls from first. Whichever
  copy finishes first counts. The loser is never cancelled: a cancel is one
  more UDP message that can be lost, and recomputing a chunk is cheaper than
  making that reliable. A dead worker is the same mechanism: its tasks go back
  into the priority queue.
- **End-to-end exactly-once.** The transport only promises at-least-once. The
  aggregator de-duplicates by `TaskID` and merges with an idempotent,
  order-free function. Every other receiver is idempotent as well.
- **Survives `kill -9` of the server.** Jobs are named by a client-chosen ID,
  not by a connection. With `-wal` the aggregator logs jobs and counted chunks;
  a restarted server replays the log, queues only the chunks it does not
  account for, workers reconnect, and the client re-asks with the same ID. The
  log is synced in 50 ms groups rather than per record, because losing a
  record only means recomputing a chunk, and that is safe for the same reason
  duplicates are safe on the wire. An end-to-end test kills the real binary
  mid-job and checks that recovered + recomputed chunks equal the job exactly.
- **Single owner, zero locks.** Every piece of mutable state belongs to one
  goroutine; goroutines exchange values over channels. Non-test code does not
  import `sync` or `sync/atomic` at all, and CI greps to keep it that way. That
  includes metrics: owners publish cumulative snapshots over channels and one
  collector goroutine serves `/metrics`. Every send towards a goroutine that
  may have ended is paired with that goroutine's `dead` channel, and the
  waits-for graph has a single direction, so there is no cycle to deadlock on.
- **No head-of-line blocking.** See above. A benchmark-only switch
  (`OrderedDelivery`, isolated in [rudp/ordered.go](rudp/ordered.go)) turns
  TCP-style ordering back on so its cost can be measured instead of asserted.
- **`Write` never blocks.** Not as an optimisation: a `Write` that waited for
  window space would stall the very goroutine that has to process the Ack that
  frees the space.

## Run it

Requires Go 1.23 or newer.

```bash
go build -o bin/ ./cmd/...
```

One server, a few workers, one client, each in its own terminal (or
backgrounded), with **10% packet loss injected into every socket**:

```bash
bin/server -port 7000 -drop 0.10
```

```bash
bin/worker -drop 0.10 localhost:7000
```

```bash
bin/client -drop 0.10 localhost:7000 "hello world" 0 500000000
```

The client prints `Result <hash> <nonce>`, or `Disconnected` if the server is
unreachable or goes away. Things to try while a job runs:

- start another worker, or kill one with `kill -9`: the answer does not change;
- start a straggler: `bin/worker -slowdown 50 -slow-after 20 localhost:7000`;
- kill the server: run it as `bin/server -port 7000 -wal jobs.wal`, the client
  with `-retries 40`, then `kill -9` the server mid-job and start it again.
  Compare `sched_chunks_recovered_total` with `sched_results_merged_total`;
- watch the scheduler work: `curl -s localhost:9100/metrics | grep -E 'sched_(speculations|tasks_requeued|results)'`;
- profile it: `go tool pprof http://localhost:9100/debug/pprof/profile?seconds=10`.

`-dup` and `-jitter-ms` add duplication and reordering; every transport and
scheduler parameter is a flag (`bin/server -h`). At loss rates above 10% raise
`-epoch-limit` on all three binaries: a connection that is only exchanging
heartbeats is declared lost after `epoch-limit` silent epochs, which happens by
chance with probability `loss^epoch-limit` per epoch.

With Docker (1 server, 4 workers, 1 client; loss rate from the environment):

```bash
LOSS=0.10 docker compose up --build
```

## Tests

```bash
go vet ./... && go test -race ./...
```

The suite covers, among others: 1000 messages in each direction at 20% drop +
10% duplication, all delivered; a lost packet not delaying the ones behind it;
an idle connection surviving on heartbeats and a vanished peer detected within
`epochLimit` epochs; `Write` not blocking on a full window and `Close` flushing;
exactly one merge per `TaskID` with 5 workers at 10% drop + 10% duplication;
killing two workers and adding one mid-job; a worker turning 50x slower; a
client vanishing mid-job with no goroutine left behind; a client reconnecting
to its running job; a server restarted from a log with a torn final record; a
log truncated at every byte offset; and end-to-end runs of the real binaries
at 10% loss and through a `kill -9` of the server.

## Benchmarks

Full tables, setup and method are in [docs/benchmarks.md](docs/benchmarks.md);
regenerate them with `go run ./cmd/bench`. Everything runs in one process over
real loopback UDP sockets, with `internal/lossy` dropping packets from a fixed
seed on every socket's send path. Each row is 5 fresh clusters x 8 measured
jobs = 40 jobs, every answer checked against the sequential result. Machine:
Apple M4 Pro (8 performance + 4 efficiency cores), workers and server sharing
it. Chunks are 100 000 nonces (about 5 ms of SHA-256 on one core) unless noted.

| Question | Result |
|---|---|
| Does it scale? (no loss) | 1 / 2 / 4 / 8 workers: 20.0 / 39.2 / 77.5 / 149.8 M nonces/s, i.e. 98% / 97% / 94% efficiency at 2 / 4 / 8 |
| Does it scale at 10% loss? | 17.2 / 33.8 / 63.6 / 113.3 M nonces/s; 8 workers reach 6.6x one worker, workers 84% to 98% busy |
| What would head-of-line blocking cost? (4 workers, 10% loss) | deliver on arrival: workers 93.5% busy, job p50 625 ms / p99 744 ms. Deliver in sequence order: 61.0% busy, p50 859 ms / p99 1035 ms |
| What does hedging buy? (one of 4 workers turns 50x slower) | speculation on: job p50 692 ms / p99 756 ms for 0.7% extra chunks. Off: p50 3022 ms / p99 3475 ms |
| How does loss hurt? (4 workers) | 0 / 5 / 10 / 20% loss: goodput 100% / 90% / 84% / 51% of loss-free, retransmit rate 0 / 11 / 23 / 57% |

One result argues against a default. With the default 10 000-nonce chunk, which
costs only 0.5 ms on this CPU, 10% loss leaves workers about 25% busy: a lost
packet parks one of the five window slots for a 20 to 40 ms retransmission
timeout, and with chunks that cheap all five are usually parked at once.
Chunks should cost at least a few milliseconds; the tables show both sizes.

## Known limitations

- **The server is a single point of failure for availability.** There is no
  replication and no failover. With `-wal` a restarted server resumes its jobs,
  but while it is down nothing progresses, and the log lives on one disk.
- **Persistence is opt-in and local.** Without `-wal`, state lives in memory
  and a restart loses every job in progress.
- **No authentication and no encryption.** Anyone who can reach the port can
  join as a worker or submit jobs; packets are plaintext and connection IDs
  are guessable. A worker's answers are trusted, not verified.
- **Flow control, but no congestion control.** The send window is fixed.
  Retransmission backs off exponentially per message, but the transport does
  not probe for or react to available bandwidth, and it is not fair to other
  traffic on a shared link.
- **One request per client connection**, and messages are limited to a single
  datagram (1200 bytes): there is no fragmentation.
- **A worker that is slow from its first chunk is not hedged.** Its task-level
  SRTT calibrates to its own pace. Pull-based balancing gives it little work,
  but the last window of chunks it holds can be a tail.
- **Failure detection is a fixed silence timeout** (100 ms by default), tuned
  for a LAN. It trades false positives on bad networks for fast recovery.

## Layout

```
rudp/            reliable-UDP transport (imports nothing from the scheduler)
sched/           dispatcher, clientHandler, workerLoop, aggregator
workload/        the Workload interface, hashsearch, a pacing wrapper
wire/            application message codec
wal/             write-ahead log: framed, checksummed, torn-tail tolerant
node/            run loops for worker and client processes
cmd/             server, worker, client, bench
internal/lossy/  net.PacketConn wrapper: seeded drop, duplication, jitter
internal/metrics collector goroutine and Prometheus rendering
e2e/             builds the binaries and runs them against each other
docs/            architecture.md, wire-format.md, benchmarks.md
```

[PLAN.md](PLAN.md) is the build plan this repository followed;
[DECISIONS.md](DECISIONS.md) records the choices the design left open.

## License

[MIT](LICENSE)
