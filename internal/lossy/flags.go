package lossy

import (
	"flag"
	"net"
	"time"
)

// Flags binds a Config to command-line flags so every binary can inject the
// same kind of network trouble into its own socket.
type Flags struct {
	Drop, Dup float64
	JitterMs  int
	Seed      int64
}

// Register adds -drop, -dup, -jitter-ms and -net-seed to fs.
func (f *Flags) Register(fs *flag.FlagSet) {
	fs.Float64Var(&f.Drop, "drop", 0, "simulate loss: probability of dropping each outgoing packet")
	fs.Float64Var(&f.Dup, "dup", 0, "simulate duplication: probability of sending an outgoing packet twice")
	fs.IntVar(&f.JitterMs, "jitter-ms", 0, "simulate reordering: delay each outgoing packet by up to this many milliseconds")
	fs.Int64Var(&f.Seed, "net-seed", 1, "seed for the simulated network")
}

// Wrapper returns a socket wrapper for rudp.Params.WrapSocket, or nil when no
// impairment was requested.
func (f *Flags) Wrapper() func(net.PacketConn) net.PacketConn {
	if f.Drop <= 0 && f.Dup <= 0 && f.JitterMs <= 0 {
		return nil
	}
	cfg := Config{Drop: f.Drop, Dup: f.Dup, Jitter: time.Duration(f.JitterMs) * time.Millisecond, Seed: f.Seed}
	return func(pc net.PacketConn) net.PacketConn { return Wrap(pc, cfg) }
}
