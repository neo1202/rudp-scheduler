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

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/wire"
)

func main() {
	var (
		p    rudp.Params
		netw lossy.Flags
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

	c, err := rudp.NewClient(addr, &p)
	if err != nil {
		fmt.Println("Disconnected")
		os.Exit(1)
	}
	defer c.Close()
	res, err := node.Submit(c, msg, lo, hi)
	if err != nil {
		fmt.Fprintln(os.Stderr, "client:", err)
		fmt.Println("Disconnected")
		c.Close()
		os.Exit(1)
	}
	fmt.Println("Result", res.Hash, res.Nonce)
}
