// Command client submits one job and prints its answer.
//
//	client [flags] <host:port> <msg> <lo> <hi>
//
// It prints "Result <hash> <nonce>" for the nonce in [lo, hi) with the smallest
// hash, or "Disconnected" if the server could not be reached or went away.
package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/transport"
	"github.com/neo1202/rudp-scheduler/wire"
	"github.com/neo1202/rudp-scheduler/workload"
)

func main() {
	var (
		p       rudp.Params
		netw    lossy.Flags
		job     = flag.Uint64("job", 0, "job ID (0 picks a random one); reuse an ID to rejoin that job")
		proto   = flag.String("transport", "rudp", "rudp or tcp; must match the server")
		retries = flag.Int("retries", 0, "after a lost connection, reconnect and ask for the same job this many times")
	)
	p.RegisterFlags(flag.CommandLine)
	netw.Register(flag.CommandLine)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: client [flags] <host:port> <msg> <lo> <hi>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 4 {
		flag.Usage()
		os.Exit(2)
	}
	addr, msg := flag.Arg(0), flag.Arg(1)
	lo, errLo := strconv.ParseUint(flag.Arg(2), 10, 64)
	hi, errHi := strconv.ParseUint(flag.Arg(3), 10, 64)
	if errLo != nil || errHi != nil {
		fmt.Fprintln(os.Stderr, "client: <lo> and <hi> must be unsigned 64-bit integers")
		os.Exit(2)
	}
	if len(msg) > wire.MaxMsgLen {
		fmt.Fprintf(os.Stderr, "client: <msg> is %d bytes, the limit is %d\n", len(msg), wire.MaxMsgLen)
		os.Exit(2)
	}
	p.WrapSocket = netw.Wrapper()

	if *job == 0 {
		*job = node.NewJobID()
	}

	// The job ID makes resubmitting safe: the server joins us to the running
	// job, or hands back the stored answer, instead of starting over.
	for attempt := 0; ; attempt++ {
		res, err := submit(*proto, addr, &p, *job, msg, lo, hi)
		if err == nil {
			fmt.Println("Result", res.Hash, res.Nonce)
			return
		}
		fmt.Fprintf(os.Stderr, "client: job %d: %v\n", *job, err)
		if attempt >= *retries {
			fmt.Println("Disconnected")
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func submit(proto, addr string, p *rudp.Params, job uint64, msg string, lo, hi uint64) (workload.Partial, error) {
	c, err := transport.Dial(proto, addr, p)
	if err != nil {
		return workload.Partial{}, err
	}
	defer c.Close()
	return node.Submit(c, job, msg, lo, hi)
}
