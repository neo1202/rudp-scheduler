// Command worker connects to a server, announces itself, and computes chunks
// until it is told to stop. If the connection is lost it reconnects.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/workload"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

func main() {
	var (
		p         rudp.Params
		netw      lossy.Flags
		costMs    = flag.Int("chunk-cost-ms", 0, "pad every chunk to at least this many milliseconds (models a slower machine)")
		slowdown  = flag.Float64("slowdown", 1, "after -slow-after chunks, take this many times longer per chunk (models a straggler)")
		slowAfter = flag.Int("slow-after", 0, "number of chunks computed at full speed before -slowdown applies")
		once      = flag.Bool("once", false, "exit when the connection ends instead of reconnecting")
	)
	p.RegisterFlags(flag.CommandLine)
	netw.Register(flag.CommandLine)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: worker [flags] <host:port>\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	addr := flag.Arg(0)
	log.SetPrefix("worker: ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	p.WrapSocket = netw.Wrapper()

	var wl workload.Workload = hashsearch.Workload{}
	if *costMs > 0 || *slowdown > 1 {
		wl = &workload.Paced{Inner: wl, MinCost: time.Duration(*costMs) * time.Millisecond, Slowdown: *slowdown, SlowAfter: *slowAfter}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	for {
		c, err := rudp.NewClient(addr, &p)
		if err != nil {
			log.Printf("connect %s: %v", addr, err)
		} else {
			log.Printf("joined %s as connection %d", addr, c.ID())
			ended := make(chan error, 1)
			go func() { ended <- node.RunWorker(c, wl) }()
			select {
			case err := <-ended:
				log.Printf("connection ended: %v", err)
				c.Close()
			case <-sig:
				log.Print("leaving")
				c.Close()
				return
			}
		}
		if *once {
			os.Exit(1)
		}
		select {
		case <-time.After(time.Second):
		case <-sig:
			return
		}
	}
}
