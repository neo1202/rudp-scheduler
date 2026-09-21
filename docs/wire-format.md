# Wire format

Everything on the wire is big-endian and fixed-layout. There is no JSON, no
varint, no optional field. This document is the reference; `rudp/packet.go` and
`wire/wire.go` implement it and their tests pin the byte layouts.

There are two layers. The transport packet is the UDP datagram. The
application message is the payload of a transport `Data` packet. The transport
never inspects a payload, and the application never sees a sequence number.

## Transport packet

One UDP datagram carries exactly one packet: an 11-byte header followed by the
payload.

```
 0        1                 5                 9          11
 +--------+-----------------+-----------------+----------+---------------+
 | type   | connID          | seq             | len      | payload       |
 | uint8  | uint32          | uint32          | uint16   | len bytes     |
 +--------+-----------------+-----------------+----------+---------------+
```

| Field | Size | Meaning |
|---|---|---|
| `type` | 1 | `1` Connect, `2` ConnAck, `3` Data, `4` Ack, `5` Close |
| `connID` | 4 | connection ID assigned by the server; `0` only in Connect |
| `seq` | 4 | sequence number (see below) |
| `len` | 2 | payload length in bytes, at most 1200 |
| `payload` | `len` | application message; empty for every type except Data |

A receiver drops a datagram when it is shorter than 11 bytes, when `type` is
not 1 to 5, when `len` differs from the number of bytes that actually follow
the header, or when `len` exceeds 1200.

### Packet types

| Type | Direction | `connID` | `seq` | Notes |
|---|---|---|---|---|
| Connect | client to server | `0` | `0` | Sent once per epoch until a ConnAck arrives or `epochLimit` epochs pass. |
| ConnAck | server to client | assigned ID | `0` | The server de-duplicates Connect by remote address: a retried Connect from the same address gets the same ID. IDs start at 1 and are never reused by a server process. |
| Data | both | the connection | `1, 2, 3, ...` | One application message. Each direction of each connection numbers its messages independently, starting at 1. A retransmission reuses the original `seq`. |
| Ack | both | the connection | the `seq` being acknowledged | Sent immediately for every Data received, duplicates included. `seq = 0` is reserved for the heartbeat: it acknowledges nothing and only proves the sender is alive. |
| Close | both | the connection | `0` | Best-effort goodbye, sent after everything written before `Close()` has been acknowledged. If it is lost, the peer finds out through the silence timeout instead. |

### Delivery semantics

Data is acknowledged and handed to the application the moment it arrives. The
receiver keeps no expected-sequence counter and no reorder buffer, and it does
not remember which sequence numbers it has already seen. The contract is:

- at least once: every message is delivered, as long as the connection lives;
- any order: `seq 7` is delivered before `seq 6` if it arrives first;
- duplicates possible: a retransmission whose original did arrive (only the Ack
  was lost) is delivered again.

Sequence numbers exist for the sender's benefit only: they key the send buffer
so an Ack can clear exactly one entry.

## Application messages

Every message starts with a one-byte `kind`.

| Kind | Name | Direction | Layout after `kind` | Total size |
|---|---|---|---|---|
| `1` | Join | worker to server | nothing | 1 |
| `2` | Request | client to server | `lo(8) hi(8) msgLen(2) msg` | 19 + msgLen |
| `3` | Chunk | server to worker | `client(4) idx(4) lo(8) hi(8) msgLen(2) msg` | 27 + msgLen |
| `4` | ChunkResult | worker to server | `client(4) idx(4) hash(8) nonce(8)` | 25 |
| `5` | Result | server to client | `hash(8) nonce(8)` | 17 |

- `lo`, `hi`: the half-open range `[lo, hi)` as unsigned 64-bit integers.
- `msg`: the job's message string, raw bytes, `msgLen` at most 1024. The
  largest message is therefore a Chunk of 1051 bytes, which fits one transport
  payload.
- `client`, `idx`: together they form the **TaskID**. `client` is the
  connection ID of the client that submitted the job; `idx` is the chunk's
  index within that job, counted from 0. The TaskID is the idempotency key of
  the whole system: it is how duplicated and speculatively re-executed chunks
  are recognised.
- `hash`, `nonce`: a partial (ChunkResult) or final (Result) answer. For the
  default hash-search workload, `hash` is the first 8 bytes, big-endian, of
  `SHA-256(msg + " " + decimal(nonce))`, and the answer is the smallest `hash`
  in range, ties broken by the smaller `nonce`. A job over an empty range
  answers with `hash = nonce = 2^64 - 1`.

A decoder rejects a message with an unknown `kind`, a truncated body, trailing
bytes, or a `msgLen` that exceeds 1024 or disagrees with the bytes present.
Rejected messages are ignored.

### Conversation

```
worker                      server                       client
  | -- Join ------------------> |                            |
  |                             | <------ Request{msg,lo,hi} |
  | <-- Chunk{client,idx,...} - |                            |
  | -- ChunkResult{client,idx,  |                            |
  |      hash,nonce} ---------> |                            |
  |            ...              |                            |
  |                             | ------ Result{hash,nonce}->|
```

A connection's first message decides its role: Join makes it a worker, Request
makes it a client. A client submits one Request per connection. Because the
transport may deliver any message twice, every receiver treats a repeated
message as a no-op: a second Join or Request on a connection is ignored, a
worker that sees the same Chunk again recomputes and resends it, and the
server counts each TaskID once.
