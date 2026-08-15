package workflow

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func mustParse(t *testing.T, src string) *Action {
	t.Helper()
	a, err := ParseYAML([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return a
}

func TestEvalFilter(t *testing.T) {
	a := mustParse(t, "name: a\nactor: subprocess\ninputs:\n"+
		"  - queue: reports.raw\n    filter: 'payload.kind == \"quarterly\" && payload.total > 0'\n"+
		"instructions:\n  command: [\"./x\"]\n")
	c, err := Compile(a)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	cases := []struct {
		payload string
		want    bool
	}{
		{`{"kind":"quarterly","total":5}`, true},
		{`{"kind":"quarterly","total":0}`, false},
		{`{"kind":"monthly","total":5}`, false},
	}
	for _, tc := range cases {
		act, err := NewActivation(json.RawMessage(tc.payload), json.RawMessage(`{"hops":0}`), "reports.raw", "t1")
		if err != nil {
			t.Fatalf("activation: %v", err)
		}
		got, err := EvalFilter(c.Filters[0], act)
		if err != nil {
			t.Fatalf("eval %s: %v", tc.payload, err)
		}
		if got != tc.want {
			t.Fatalf("filter(%s) = %t, want %t", tc.payload, got, tc.want)
		}
	}

	// A missing field is an evaluation error, not a silent false: a broken
	// filter must be loud.
	act, err := NewActivation(json.RawMessage(`{}`), nil, "reports.raw", "t1")
	if err != nil {
		t.Fatalf("activation: %v", err)
	}
	if _, err := EvalFilter(c.Filters[0], act); err == nil {
		t.Fatal("expected an evaluation error for a missing field")
	}
}

// An absent filter accepts everything; nothing should have to special-case it.
func TestEvalFilterNilProgramAccepts(t *testing.T) {
	act, err := NewActivation(json.RawMessage(`{}`), nil, "q", "t")
	if err != nil {
		t.Fatalf("activation: %v", err)
	}
	ok, err := EvalFilter(nil, act)
	if err != nil || !ok {
		t.Fatalf("nil filter: ok=%t err=%v", ok, err)
	}
}

func TestEvalCorrelate(t *testing.T) {
	a := mustParse(t, "name: a\nactor: subprocess\ninputs:\n"+
		"  - queue: q1\n    correlate_on: 'payload.order_id'\n"+
		"  - queue: q2\n    correlate_on: 'payload.order_id'\n"+
		"instructions:\n  command: [\"./x\"]\n")
	c, err := Compile(a)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	// A string key and an int key coerce to the same correlation key, so a
	// producer spelling the id either way still joins.
	for _, payload := range []string{`{"order_id":"42"}`, `{"order_id":42}`} {
		act, err := NewActivation(json.RawMessage(payload), nil, "q1", "t")
		if err != nil {
			t.Fatalf("activation: %v", err)
		}
		key, err := EvalCorrelate(c.Correlate[0], act)
		if err != nil {
			t.Fatalf("correlate %s: %v", payload, err)
		}
		if key != "42" {
			t.Fatalf("correlate(%s) = %q, want \"42\"", payload, key)
		}
	}

	// A dyn expression that yields a non-key type only fails at eval time,
	// which is exactly the case the compile-time check cannot reach.
	act, err := NewActivation(json.RawMessage(`{"order_id":{"nested":1}}`), nil, "q1", "t")
	if err != nil {
		t.Fatalf("activation: %v", err)
	}
	if _, err := EvalCorrelate(c.Correlate[0], act); err == nil {
		t.Fatal("expected an error for a map correlation key")
	} else if !strings.Contains(err.Error(), "want string or int") {
		t.Fatalf("unhelpful error: %v", err)
	}
}

func TestCompileLeavesUnusedSlotsNil(t *testing.T) {
	a := mustParse(t, "name: a\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  command: [\"./x\"]\n")
	c, err := Compile(a)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(c.Filters) != 1 || c.Filters[0] != nil || c.Correlate[0] != nil {
		t.Fatalf("expected nil programs for absent expressions: %+v", c)
	}
}

// The cache is read by the scheduler from several goroutines; a torn map or a
// duplicate compile under contention is the failure mode being tested.
func TestCacheConcurrent(t *testing.T) {
	a := mustParse(t, "name: a\nactor: subprocess\ninputs:\n"+
		"  - queue: q\n    filter: 'payload.n > 0'\ninstructions:\n  command: [\"./x\"]\n")
	cache := NewCache()

	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			version := int64(i%4) + 1
			c, err := cache.Get("a", version, a)
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			if c.Filters[0] == nil {
				t.Error("cached entry missing its filter program")
			}
		}()
	}
	wg.Wait()

	if got := cache.Len(); got != 4 {
		t.Fatalf("cached %d entries, want 4", got)
	}
}
