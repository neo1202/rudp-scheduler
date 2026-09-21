# Plan

A fault-tolerant distributed job scheduler running on a home-grown reliable-UDP
transport. Each milestone ends with its tests passing under `go test -race` and a
commit.

| # | Milestone | Deliverable | Tests that gate it |
|---|---|---|---|
| 1 | `internal/lossy` | `net.PacketConn` wrapper: drop %, dup %, delay jitter, seeded RNG, per-packet filter. Single goroutine owns the RNG and the delay heap. | drop/dup rates converge to the configured values; same seed gives the same decision sequence; filter can drop a chosen packet; `Close` flushes delayed packets |
| 2 | `rudp` | packet codec, per-connection `mainLoop`, server `readLoop`, client `readLoop`, handshake, heartbeat, RTO/backoff/Karn, `OrderedDelivery` isolated in one file | codec round-trip + malformed input; 1000 messages at 20 % drop + 10 % dup all delivered at least once; no head-of-line blocking (drop first transmission of seq=k, later seqs arrive first); ordered mode delivers strictly in order; idle connection survives 50 epochs; vanished peer reported within `epochLimit` epochs; `Write` never blocks on a full window; `Close` flushes `pendingWrites` |
| 3 | `workload` | `Workload` interface, `hashsearch`, `Paced` wrapper | known-answer hash test; `Compute` over split ranges merges to the sequential answer; `testing/quick` properties for `Merge`: commutative, associative, idempotent, identity |
| 4 | `wire` | fixed big-endian application messages, `docs/wire-format.md` | round-trip for all five kinds; truncated / oversized / trailing-byte inputs rejected |
| 5 | `sched` + `node` | dispatcher / clientHandler / workerLoop / aggregator, worker and client run loops | exactly-once under 10 % drop + 10 % dup with 5 workers (result equals sequential answer, one merge per TaskID); kill 2 workers + add 1 mid-job; 50x slow worker does not hold the job (speculation fires, each inflight cloned at most once); client vanishes mid-job (job dropped, late results discarded, no goroutine leak); invariants 1 and 6 asserted through hooks |
| 6 | `cmd/*` | `server`, `worker`, `client` binaries, flags for every parameter and for loss injection | end-to-end test that builds and runs the three binaries on localhost with 10 % loss |
| 7 | metrics | `/metrics` (Prometheus text) + `/debug/pprof`; a single metrics goroutine owns all series, fed by snapshots over channels | snapshot rendering test; scrape during a live job shows the expected series |
| 8 | `cmd/bench` | four reproducible experiments on `internal/lossy`, fixed seeds, repeated runs, Markdown written to `docs/benchmarks.md` | bench smoke test with tiny sizes |
| 9 | packaging | Dockerfile, docker-compose (1 server + 4 workers + 1 client, loss via env), README, `docs/architecture.md`, LICENSE, CI (vet + race tests + no-locks grep) | `docker compose config`; CI workflow lint by running the same commands locally |

Static gates that hold at every commit:

- `go vet ./...` clean
- `go test -race ./...` clean
- `grep -rE 'sync\.(Mutex|RWMutex|Map)|sync/atomic' --include='*.go' --exclude='*_test.go' .` prints nothing
