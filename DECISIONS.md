# Decisions

Choices that the design left open, one per line, each with its reason. The
first section lists the places where the implementation knowingly differs from
the design it was built from.

## Departures from the original design

- **Speculated tasks do not feed the task-level SRTT.** The design samples every returning result; here a result whose task was already cloned is skipped, the same idea as Karn's rule one layer down. Reason: a degraded worker otherwise drags its own SRTT up within a handful of samples, the `3 x SRTT` threshold follows it, and hedging quietly switches itself off exactly when it is needed. Measured with the setup of benchmark 3 (4 workers, one degraded 50x, 40 jobs): sampling every result gives p50 2861 ms with 0 clones per job, against 3022 ms with speculation disabled; skipping speculated tasks gives p50 692 ms. Cost: a worker that becomes permanently slower stays hedged forever, so its capacity is mostly wasted; that is bounded by one worker's throughput.
- **A lost connection hands its delivery backlog up before the error.** The design reports the loss at once and discards what `deliverQ` still holds; here `dead` is still closed first (so nobody can block on the connection), then the backlog and finally the error go to `Read()`. Reason: those messages were already acknowledged to the peer, and for a worker they are finished results that would otherwise be recomputed.
- **Invariant 6 ("a worker connection never has pendingWrites") is only guaranteed without loss or reordering.** If the Ack of a Chunk is lost but the ChunkResult arrives, the scheduler frees the in-flight slot while the transport still holds the unacknowledged Chunk, so the next Chunk can wait in `pendingWrites` for one retransmission round. The excess is bounded by the window and in-flight work stays bounded by invariant 1. The test asserts invariant 6 on a loss-free network and invariant 1 everywhere.

## Transport

- **Packet payloads above 1200 bytes make `Write` panic.** Reason: `Write` has no error return by contract, and silently dropping would turn a programming error into a hang.
- **`Write` copies the payload.** Reason: the caller may reuse its buffer; the copy is cheaper than a data race.
- **`inCh` is buffered (64).** Reason: the server has a single read loop; a buffer keeps one momentarily busy connection from delaying every other connection's packets. Sends to it still pair with `<-dead`.
- **The read loop sends ConnAck itself and owns the address table next to the connection table.** Reason: Connect de-duplication is a table lookup, and both tables must have the same single owner. A Connect from an address whose previous connection has ended gets a fresh connection ID; IDs are never reused.
- **A Close packet is sent once, unreliably, after the flush.** Reason: making goodbye reliable needs its own retransmission state; if it is lost the peer's silence timeout does the same job a few epochs later. The receiver reports it as `ErrConnClosed`, after handing up its backlog.
- **A local `Close()` does not produce an error from `Read()`.** Reason: the application asked for it; only connections that end on their own are reported.
- **While flushing after `Close()`, incoming data is acknowledged but no longer delivered, and late `Write`s are discarded.** Reason: the application has left; acknowledging lets the peer's own flush finish.
- **`Server.Close()` flushes every connection before releasing the socket, and the read loop keeps routing Acks while it waits.** Reason: otherwise a Result written just before shutdown could be lost.
- **RTO is checked once per epoch against the time of the last transmission.** Reason: one ticker instead of a timer per message; the effective timeout is therefore the configured RTO rounded up to the next epoch boundary.
- **The heartbeat for an epoch is sent at the tick that ends it.** Reason: that is when the connection knows nothing else was sent.
- **Ordered delivery is implemented by swapping one function value (`conn.accept`).** Reason: the default path contains no sequence-order logic at all, and the benchmark-only code lives entirely in `rudp/ordered.go`.
- **Transport parameters live in `rudp.Params`, scheduler parameters in `sched.Params`, which embeds the former.** Reason: one struct configures a server, yet the transport package does not learn words like "chunk". `Window` is read by both layers, which is the design's one deliberate coupling.
- **Go 1.23 is the minimum.** Reason: timer and ticker channels are unbuffered from 1.23 on, which removes stale ticks after `Reset`/`Stop`.

## Scheduler

- **A worker that pulls its own clone from `pq` puts it back and ignores `pq` until its next tick.** Reason: recording it would overwrite the in-flight entry, reset its age and send the same chunk to the same slow worker. If `pq` is full the clone is dropped; the original is still in flight, so the task is not lost and the entry stays marked as speculated (invariant 2).
- **A job's feeder stops when the aggregator drops the job (a per-job `cancel` channel), and a Request whose client has already vanished still registers the job, as orphaned.** Reason: without the first, an abandoned job still costs a full job of wasted work; the second closes the race where the dispatcher's "client gone" notice overtakes the clientHandler, so the grace period always starts. `ConnHandle.Done()` was added to the transport API for this check.
- **A connection's first message fixes its role.** A Join on a client connection or a Request on a worker connection is ignored. Reason: repeated messages must be no-ops, and mixed roles have no meaning.
- **One request per client connection.** Reason: it keeps "which job is this connection waiting for" a single value; a client that wants several jobs opens several connections.
- **Jobs are capped at 2^20 tasks; larger ranges get proportionally larger chunks.** Reason: bounds the aggregator's `done` map and keeps the chunk index inside 32 bits.
- **An empty or inverted range is answered at once with `Zero()`.** Reason: a job with zero tasks would otherwise never complete.
- **The aggregator checks that a result's chunk index is inside the job.** Reason: a faulty worker must not be able to make `remaining` reach zero early.
- **`MaxWorkers` (default 64) only sizes `pq`; it is not an admission limit.** Reason: a full `pq` delays a clone by one tick and never affects correctness.
- **`Partial` is a fixed pair of `uint64` (`Hash`, `Nonce`), and `Zero()` for hash search is `{MaxUint64, MaxUint64}`.** Reason: the wire format stays fixed-size, and that value is the identity of a lexicographic minimum. Workloads with richer partial results would need a wire-format revision.
- **Job messages are limited to 1024 bytes.** Reason: the largest message, a Chunk, must fit one 1200-byte transport payload.

## Durability

- **`TaskID` is `{job ID, chunk index}` with a client-chosen 64-bit job ID, not `{client connection, chunk index}` as first designed; Request, Chunk, ChunkResult and Result carry the job ID.** Reason: a connection-derived ID dies with the connection, so neither a reconnecting client nor a restarted server could say which job they meant. This is a wire-format change.
- **A client losing its connection no longer drops the job at once; the job waits `jobGraceMs` (5 s) for the same job ID to come back.** Reason: reattachment is what makes a restart survivable, and it also rescues a client that was only declared lost by a run of dropped heartbeats. Cost: up to 5 s of work for a client that is really gone.
- **The log is synced in groups (every 50 ms), and only `JobStart` and `JobDone` are synced immediately.** Reason: a lost `ChunkDone` costs one recomputed chunk and cannot change the answer, because `Compute` is deterministic and `Merge` idempotent. Correctness never depends on a record having reached the disk.
- **The clientHandler asks the aggregator before splitting** (`submitCh`, with a reply) instead of announcing a new job. Reason: only the aggregator knows whether a job ID is new, running, or already answered, and only a new job needs a feeder.
- **Finished answers are kept for `resultTtlMs` (60 s) and logged.** Reason: a client whose Result was lost with its connection must be able to ask again without the job being recomputed.
- **Log compaction happens at start and when the scheduler is idle, never mid-job.** Reason: `done` holds only TaskIDs, not the partial results a running job's `ChunkDone` records would have to repeat; compacting when nothing is running needs no extra state.
- **A failed log write is counted (`sched_wal_errors_total`) and otherwise ignored.** Reason: the scheduler is correct without the log; a full disk should cost durability, not the running jobs.
- **The server's read loop drops a packet whose source address is not the connection's peer.** Reason: connection IDs restart from 1 when the server does, so a straggling packet from before the restart could otherwise land on a new connection with the same ID.

## Observability

- **Metrics are cumulative snapshots sent with non-blocking sends, not one event per increment.** Reason: a dropped snapshot is repaired by the next one, so the data path never waits for the metrics goroutine and counters still never go backwards. Series whose owner disappears without a final snapshot are retired after 5 s.
- **Queue depths are read with `len(chan)` at scrape time.** Reason: the channels are the shared objects; asking them is cheaper and more accurate than mirroring their length in a counter.
- **Server-side worker utilization is "time with at least one task in flight".** Reason: it is what the server can observe. It overstates a worker that is blocked behind a lost packet, which is why the benchmark measures real compute time inside the workers instead.

## Testing and tooling

- **`internal/lossy` impairs the send path only, from one goroutine that owns the RNG.** Reason: wrapping both endpoints impairs both directions, and a single owner needs no lock. "Reproducible" means each socket sees the same decision sequence for the same seed; the interleaving of goroutines, and so the exact packet that meets each decision, still varies from run to run.
- **Lossy tests raise `EpochLimit`.** Reason: a connection that carries only heartbeats is declared lost with probability `p^EpochLimit` per epoch; with the default of 5 that is 1 in 3000 epochs at 20% loss, which is a property of the parameter, not a bug, but would make tests flaky.
- **No third-party dependencies; goroutine leaks are checked with `runtime.NumGoroutine`.** Reason: a zero-dependency module is easier to audit and build.
- **Non-test code avoids the whole `sync` package, not just the forbidden types.** Reason: `Once` and `WaitGroup` would pass the grep but blur the rule; idempotent `Close` methods are built from a capacity-1 channel or a request channel instead.
- **`wire`, `node` and `e2e` are packages beyond the planned layout.** Reason: the worker and client binaries need the message codec without importing the server's scheduler; the run loops are shared by binaries, tests and benchmarks; the end-to-end test builds real binaries.
- **The worker binary reconnects by default (`-once` disables it).** Reason: in `docker compose` a worker may start before the server, and a falsely declared loss should not remove capacity for good.
- **Benchmarks use `EpochLimit = 10`, 100 000-nonce chunks outside the scaling tables, and a straggler that degrades after 10 chunks.** Reasons: see the header of `docs/benchmarks.md`. The last one matters: a worker that is slow from its very first chunk calibrates its own SRTT to its own pace and is handled by pull-based balancing alone, not by speculation.
