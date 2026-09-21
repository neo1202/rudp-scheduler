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
	cmd := exec.Command(bin, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
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
	return stderr
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
	portRE := regexp.MustCompile(`listening on udp port (\d+)`)
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
