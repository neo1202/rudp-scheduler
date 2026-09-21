// Package e2e builds the three binaries and runs them against each other on
// localhost, the way a user would.
package e2e

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/neo1202/rudp-scheduler/workload/hashsearch"
)

func build(t *testing.T, dir string, names ...string) map[string]string {
	t.Helper()
	bins := map[string]string{}
	for _, name := range names {
		out := filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", out, "github.com/neo1202/rudp-scheduler/cmd/"+name)
		if b, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", name, err, b)
		}
		bins[name] = out
	}
	return bins
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// start runs a binary and terminates it gracefully when the test ends.
func start(t *testing.T, bin string, args ...string) io.Reader {
	t.Helper()
	_, stderr := startCmd(t, bin, args...)
	return stderr
}

func startCmd(t *testing.T, bin string, args ...string) (*exec.Cmd, io.Reader) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState != nil {
			return // already reaped by the test
		}
		cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			cmd.Process.Kill()
			t.Errorf("%s did not exit on SIGTERM", filepath.Base(bin))
		}
	})
	return cmd, stderr
}

func scrape(t *testing.T, addr string) string {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func metricValue(t *testing.T, text, series string) float64 {
	t.Helper()
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, series+" ") {
			v, err := strconv.ParseFloat(strings.TrimPrefix(line, series+" "), 64)
			if err != nil {
				t.Fatalf("%s: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("series %s not found in /metrics", series)
	return 0
}

func TestBinariesUnderTenPercentLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	bins := build(t, t.TempDir(), "server", "worker", "client")
	httpAddr := freeTCPAddr(t)

	// A roomier silence limit than the default, so that a run of lost
	// heartbeats on a loaded CI machine does not end the client's connection.
	common := []string{"-drop", "0.10", "-epoch-limit", "25"}

	serverLog := start(t, bins["server"], append([]string{"-port", "0", "-http", httpAddr, "-net-seed", "1"}, common...)...)
	portRE := regexp.MustCompile(`listening on \w+ port (\d+)`)
	var addr string
	sc := bufio.NewScanner(serverLog)
	for sc.Scan() {
		if m := portRE.FindStringSubmatch(sc.Text()); m != nil {
			addr = "127.0.0.1:" + m[1]
			break
		}
	}
	if addr == "" {
		t.Fatal("server never announced its port")
	}
	go io.Copy(io.Discard, serverLog)

	for i := 0; i < 3; i++ {
		w := start(t, bins["worker"], append([]string{"-net-seed", fmt.Sprint(10 + i)}, append(common, addr)...)...)
		go io.Copy(io.Discard, w)
	}

	const msg, lo, hi = "end to end", 0, 12_000_000 // 1200 chunks of 10 000
	args := append([]string{"-net-seed", "99"}, append(common, addr, msg, fmt.Sprint(lo), fmt.Sprint(hi))...)
	client := exec.Command(bins["client"], args...)
	client.Stderr = os.Stderr
	out, err := client.Output()
	if err != nil {
		t.Fatalf("client: %v (stdout %q)", err, out)
	}
	want := hashsearch.Workload{}.Compute(msg, lo, hi)
	if got, expect := strings.TrimSpace(string(out)), fmt.Sprintf("Result %d %d", want.Hash, want.Nonce); got != expect {
		t.Errorf("client printed %q, want %q", got, expect)
	}

	text := scrape(t, httpAddr)
	if v := metricValue(t, text, `sched_jobs_total{outcome="completed"}`); v != 1 {
		t.Errorf("completed jobs = %v, want 1", v)
	}
	if v := metricValue(t, text, "sched_results_merged_total"); v != 1200 {
		t.Errorf("merged results = %v, want 1200 (exactly one per chunk)", v)
	}
	if v := metricValue(t, text, "rudp_retransmits_total"); v == 0 {
		t.Error("no retransmissions at 10% loss: is the loss injection wired up?")
	}
	if v := metricValue(t, text, "sched_workers"); v != 3 {
		t.Errorf("workers = %v, want 3", v)
	}
	if v := metricValue(t, text, "sched_job_latency_seconds_count"); v != 1 {
		t.Errorf("latency histogram count = %v, want 1", v)
	}
	t.Logf("retransmits=%v first-sends=%v duplicates-at-aggregator=%v speculations=%v",
		metricValue(t, text, "rudp_retransmits_total"), metricValue(t, text, "rudp_data_sent_total"),
		metricValue(t, text, "sched_results_duplicate_total"), metricValue(t, text, "sched_speculations_total"))

	resp, err := http.Get("http://" + httpAddr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/debug/pprof/goroutine: status %d", resp.StatusCode)
	}
}

func TestClientPrintsDisconnected(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	bins := build(t, t.TempDir(), "client")
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := pc.LocalAddr().String()
	pc.Close()

	out, err := exec.Command(bins["client"], dead, "nobody home", "0", "100").Output()
	if err == nil {
		t.Error("client exited 0 without a server")
	}
	if got := strings.TrimSpace(string(out)); got != "Disconnected" {
		t.Errorf("client printed %q, want %q", got, "Disconnected")
	}
}

func freeUDPPort(t *testing.T) int {
	t.Helper()
	pc, err := net.ListenPacket("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	return pc.LocalAddr().(*net.UDPAddr).Port
}

// kill -9 the server in the middle of a job and start a new one on the same
// port and the same log. Workers reconnect on their own, the client asks
// again with the same job ID, and the job finishes without redoing the chunks
// the log remembers.
func TestServerKilledAndRestarted(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs binaries")
	}
	bins := build(t, t.TempDir(), "server", "worker", "client")
	port, httpAddr := freeUDPPort(t), freeTCPAddr(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	walPath := filepath.Join(t.TempDir(), "jobs.wal")
	serverArgs := []string{"-port", fmt.Sprint(port), "-http", httpAddr, "-wal", walPath, "-job-grace-ms", "20000"}

	first, firstLog := startCmd(t, bins["server"], serverArgs...)
	go io.Copy(io.Discard, firstLog)
	for i := 0; i < 3; i++ {
		go io.Copy(io.Discard, start(t, bins["worker"], "-chunk-cost-ms", "5", addr))
	}

	const msg, lo, hi, chunks = "kill -9", 0, 6_000_000, 600
	client := exec.Command(bins["client"], "-job", "12345", "-retries", "40", addr, msg, fmt.Sprint(lo), fmt.Sprint(hi))
	var out strings.Builder
	client.Stdout = &out
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for merged := 0.0; merged < chunks/3; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("first server made no progress")
		}
		if resp, err := http.Get("http://" + httpAddr + "/metrics"); err == nil { // the HTTP listener may not be up yet
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			merged = metricValue(t, string(b), "sched_results_merged_total")
		}
	}
	first.Process.Kill() // SIGKILL: no flush, no goodbye
	first.Wait()

	go io.Copy(io.Discard, start(t, bins["server"], serverArgs...))

	done := make(chan error, 1)
	go func() { done <- client.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("client: %v (stdout %q)", err, out.String())
		}
	case <-time.After(60 * time.Second):
		client.Process.Kill()
		t.Fatal("client never got its result after the restart")
	}
	want := hashsearch.Workload{}.Compute(msg, lo, hi)
	if got, expect := strings.TrimSpace(out.String()), fmt.Sprintf("Result %d %d", want.Hash, want.Nonce); got != expect {
		t.Errorf("client printed %q, want %q", got, expect)
	}

	text := scrape(t, httpAddr)
	recovered := metricValue(t, text, "sched_chunks_recovered_total")
	redone := metricValue(t, text, "sched_results_merged_total")
	if metricValue(t, text, "sched_jobs_recovered_total") != 1 || recovered == 0 {
		t.Errorf("second server recovered %v chunks; want the job and some chunks back from the log", recovered)
	}
	if recovered+redone != chunks {
		t.Errorf("recovered %v + recomputed %v != %d chunks", recovered, redone, chunks)
	}
	t.Logf("after kill -9: %v chunks came back from the log, %v were recomputed", recovered, redone)
}
