package scheduler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mcondie/sonata/internal/store"
)

func TestBackoffScheduleAndJitterBounds(t *testing.T) {
	s := &Scheduler{cfg: Config{BackoffBase: time.Second, BackoffCap: time.Minute}}
	cases := []struct {
		attempt int
		nominal time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{7, 60 * time.Second},  // capped
		{80, 60 * time.Second}, // shift overflow guarded, still capped
	}
	for _, tc := range cases {
		for i := 0; i < 50; i++ {
			d := s.backoff(tc.attempt)
			lo := time.Duration(float64(tc.nominal) * 0.8)
			hi := time.Duration(float64(tc.nominal) * 1.2)
			if d < lo || d > hi {
				t.Fatalf("backoff(%d) = %s, want within [%s, %s]", tc.attempt, d, lo, hi)
			}
		}
	}
}

func TestParseNDJSON(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    int
		wantErr bool
	}{
		{"empty", "", 0, false},
		{"blank lines only", "\n\n  \n", 0, false},
		{"one object", `{"a":1}`, 1, false},
		{"three lines with blanks", "{\"a\":1}\n\n{\"b\":2}\n{\"c\":3}\n", 3, false},
		{"scalar payloads are JSON too", "1\n\"two\"\n", 2, false},
		{"garbage line", "{\"a\":1}\nnot json\n", 0, true},
		{"truncated object", `{"a":`, 0, true},
	}
	for _, tc := range cases {
		got, err := parseNDJSON([]byte(tc.in))
		if tc.wantErr != (err != nil) {
			t.Errorf("%s: err = %v, wantErr %v", tc.name, err, tc.wantErr)
			continue
		}
		if !tc.wantErr && len(got) != tc.want {
			t.Errorf("%s: %d lines, want %d", tc.name, len(got), tc.want)
		}
	}
}

func TestMessageHops(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{`{"hops":0}`, 0, false},
		{`{"hops":7}`, 7, false},
		{`{}`, 0, false},
		{``, 0, false},
		{`{"hops":"three"}`, 0, true},
	}
	for _, tc := range cases {
		got, err := messageHops(json.RawMessage(tc.in))
		if tc.wantErr != (err != nil) || got != tc.want {
			t.Errorf("messageHops(%q) = %d, %v; want %d, err=%v", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestEnvelopeShape(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("echoer", "q", "cmd", ""))
	h.exec.respond("cmd", "", nil)
	m := h.send("q", `{"a":1}`, "")

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 1)

	inputs := h.exec.inputs("cmd")
	if len(inputs) != 1 {
		t.Fatalf("executions: %d", len(inputs))
	}
	var env struct {
		ID      string          `json:"id"`
		Queue   string          `json:"queue"`
		Payload json.RawMessage `json:"payload"`
		Headers json.RawMessage `json:"headers"`
		TraceID string          `json:"trace_id"`
	}
	if err := json.Unmarshal(inputs[0], &env); err != nil {
		t.Fatalf("stdin is not one JSON line: %v (%q)", err, inputs[0])
	}
	if env.ID != m.ID || env.Queue != "q" || env.TraceID != m.TraceID ||
		string(env.Payload) != `{"a":1}` {
		t.Errorf("envelope = %+v", env)
	}
}
