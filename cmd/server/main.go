// Command server runs the scheduler: it accepts workers and clients on one UDP
// port, and serves /metrics and /debug/pprof over HTTP.
package main

import (
	"flag"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/neo1202/rudp-scheduler/internal/lossy"
	"github.com/neo1202/rudp-scheduler/internal/metrics"
	"github.com/neo1202/rudp-scheduler/rudp"
	"github.com/neo1202/rudp-scheduler/sched"
	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

func main() {
	var (
		p     sched.Params
		netw  lossy.Flags
		port  = flag.Int("port", 7000, "UDP port to listen on (0 picks a free one)")
		httpA = flag.String("http", ":9100", "address for /metrics and /debug/pprof (empty disables)")
	)
	p.RegisterFlags(flag.CommandLine)
	netw.Register(flag.CommandLine)
	flag.Parse()
	log.SetPrefix("server: ")
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	col := metrics.New()
	p.Transport.Stats = col.ConnStats()
	p.WorkerStats = col.WorkerStats()
	p.AggStats = col.AggStats()
	p.Transport.WrapSocket = netw.Wrapper()

	srv, err := rudp.NewServer(*port, &p.Transport)
	if err != nil {
		log.Fatal(err)
	}
	s, err := sched.New(srv, hashsearch.Workload{}, &p)
	if err != nil {
		log.Fatal(err)
	}
	col.Start(s.QueueDepths)
	log.Printf("listening on udp port %d (drop=%.2f dup=%.2f jitter=%dms wal=%q)", srv.Port(), netw.Drop, netw.Dup, netw.JitterMs, p.WALPath)

	if *httpA != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", col.Handler())
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		go func() {
			log.Printf("metrics and pprof on http://%s", *httpA)
			if err := http.ListenAndServe(*httpA, mux); err != nil {
				log.Printf("http: %v", err)
			}
		}()
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Print("shutting down")
		srv.Close()
	}()

	s.Run() // returns once the server is closed
	col.Stop()
}
