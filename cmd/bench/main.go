// Command bench runs the repository's four experiments and writes the results
// as Markdown. Everything happens in one process on localhost: a real server,
// real workers and real clients talking over real UDP sockets, with
// internal/lossy impairing each socket's outgoing packets from a fixed seed.
//
//	go run ./cmd/bench -out docs/benchmarks.md
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type options struct {
	out   string
	seed  int64
	quick bool
	only  string
}

func main() {
	var o options
	flag.StringVar(&o.out, "out", "docs/benchmarks.md", "file to write the Markdown report to (\"-\" for stdout)")
	flag.Int64Var(&o.seed, "seed", 20260921, "base seed for every simulated network")
	flag.BoolVar(&o.quick, "quick", false, "tiny sizes, for smoke testing; numbers are meaningless")
	flag.StringVar(&o.only, "only", "", "comma-separated subset of: scaling,hol,hedging,loss")
	flag.Parse()
	log.SetFlags(log.Ltime)

	start := time.Now()
	report := run(o)
	log.Printf("all experiments done in %v", time.Since(start).Round(time.Second))

	if o.out == "-" {
		fmt.Print(report)
		return
	}
	if err := os.WriteFile(o.out, []byte(report), 0o644); err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %s", o.out)
}

func run(o options) string {
	sz := fullSizes
	if o.quick {
		sz = quickSizes
	}
	want := func(name string) bool {
		return o.only == "" || strings.Contains(","+o.only+",", ","+name+",")
	}

	var b strings.Builder
	writeHeader(&b, o, sz)
	if want("scaling") {
		scaling(&b, o, sz)
	}
	if want("hol") {
		headOfLine(&b, o, sz)
	}
	if want("hedging") {
		hedging(&b, o, sz)
	}
	if want("loss") {
		lossSweep(&b, o, sz)
	}
	return b.String()
}
