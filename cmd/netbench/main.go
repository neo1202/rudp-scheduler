// Command netbench submits the same job several times, one connection per
// job, and prints one Markdown table row of timings. It is the client side of
// the netem experiment (see netem/), where the network trouble comes from the
// kernel rather than from this repository's own simulator.
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/neo1202/rudp-scheduler/node"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/transport"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

func main() {
	var (
		p      rudp.Params
		proto  = flag.String("transport", "rudp", "rudp or tcp; must match the server")
		jobs   = flag.Int("jobs", 10, "measured jobs, after one unmeasured warm-up job")
		nonces = flag.Uint64("nonces", 40_000_000, "size of each job")
		label  = flag.String("label", "", "first cell of the output row")
		limit  = flag.Duration("timeout", 5*time.Minute, "give up on the whole run after this long")
	)
	p.RegisterFlags(flag.CommandLine)
	flag.Parse()
	if flag.NArg() != 1 {
		log.Fatal("usage: netbench [flags] <host:port>")
	}
	addr := flag.Arg(0)
	log.SetFlags(log.Ltime)
	time.AfterFunc(*limit, func() { log.Fatalf("| %s | timed out after %v |", *label, *limit) })

	const msg = "netem"
	want := hashsearch.Workload{}.Compute(msg, 0, *nonces)

	var times []time.Duration
	failed := 0
	for i := -1; i < *jobs; i++ { // job -1 is the warm-up
		took, err := one(*proto, addr, &p, msg, *nonces, want)
		switch {
		case err != nil:
			log.Printf("job %d: %v", i, err)
			failed++
		case i >= 0:
			times = append(times, took)
		}
	}
	if len(times) == 0 {
		log.Fatalf("| %s | every job failed |", *label)
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	var sum time.Duration
	for _, d := range times {
		sum += d
	}
	mean := sum / time.Duration(len(times))
	fmt.Printf("| %s | %.0f ms | %.0f ms | %.0f ms | %.1f | %d | %d |\n", *label,
		ms(times[len(times)/2]), ms(times[len(times)-1]), ms(mean),
		float64(*nonces)/mean.Seconds()/1e6, len(times), failed)
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// one runs a single job, retrying the same job ID if the connection is lost,
// exactly as the client binary does. The time includes those retries.
func one(proto, addr string, p *rudp.Params, msg string, nonces uint64, want any) (time.Duration, error) {
	job := node.NewJobID()
	start := time.Now()
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		c, err := transport.Dial(proto, addr, p)
		if err != nil {
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		}
		res, err := node.Submit(c, job, msg, 0, nonces)
		c.Close()
		if err == nil {
			if res != want {
				return 0, fmt.Errorf("wrong answer %+v, want %+v", res, want)
			}
			return time.Since(start), nil
		}
		lastErr = err
	}
	return 0, lastErr
}
