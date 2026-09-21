package main

import (
	"strings"
	"testing"
)

// The harness itself has to keep working; the numbers of a -quick run mean
// nothing, but every job in it is still checked against the sequential answer.
func TestQuickRunProducesEveryTable(t *testing.T) {
	if testing.Short() {
		t.Skip("runs clusters")
	}
	report := run(options{seed: 1, quick: true})
	for _, want := range []string{
		"## Setup", "## 1. Scaling", "## 2. Head-of-line blocking",
		"## 3. Hedging against a straggler", "## 4. Loss sweep",
		"| on arrival (default) |", "| in sequence order |", "| on (default) |", "| off |",
	} {
		if !strings.Contains(report, want) {
			t.Errorf("report is missing %q", want)
		}
	}
	if strings.Contains(report, "NaN") || strings.Contains(report, "Inf") {
		t.Errorf("report contains a non-finite number:\n%s", report)
	}
}
