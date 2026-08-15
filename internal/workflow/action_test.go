package workflow

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const minimalYAML = `
name: close-order
inputs:
  - queue: invoices.approved
actor: subprocess
instructions:
  command: ["./close.sh"]
`

func TestParseYAMLFull(t *testing.T) {
	const src = `
name: close-order
inputs:
  - queue: invoices.approved
    filter: 'payload.total > 0'
    correlate_on: 'payload.order_id'
  - queue: shipments.confirmed
    correlate_on: 'payload.order_id'
actor: subprocess
instructions:
  command: ["./close.sh", "--verbose"]
  timeout: 300s
output: orders.closed
concurrency: 4
max_attempts: 3
join_timeout: 24h
`
	a, err := ParseYAML([]byte(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if a.Name != "close-order" || a.Actor != ActorSubprocess || a.Output != "orders.closed" {
		t.Fatalf("unexpected header fields: %+v", a)
	}
	if got := a.Queues(); len(got) != 2 || got[0] != "invoices.approved" || got[1] != "shipments.confirmed" {
		t.Fatalf("queues = %v", got)
	}
	if !a.IsJoin() {
		t.Fatal("expected a join")
	}
	if a.ConcurrencyOrDefault() != 4 || *a.MaxAttempts != 3 {
		t.Fatalf("concurrency/max_attempts = %d/%d", a.ConcurrencyOrDefault(), *a.MaxAttempts)
	}
	if a.JoinTimeoutOrDefault() != 24*time.Hour {
		t.Fatalf("join_timeout = %s", a.JoinTimeoutOrDefault())
	}

	si, err := a.Subprocess()
	if err != nil {
		t.Fatalf("subprocess instructions: %v", err)
	}
	if len(si.Command) != 2 || si.Command[0] != "./close.sh" {
		t.Fatalf("command = %v", si.Command)
	}
	if si.Timeout == nil || time.Duration(*si.Timeout) != 5*time.Minute {
		t.Fatalf("timeout = %v", si.Timeout)
	}
}

func TestDefaultsWhenUnset(t *testing.T) {
	a, err := ParseYAML([]byte(minimalYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if a.Concurrency != nil || a.MaxAttempts != nil || a.JoinTimeout != nil {
		t.Fatalf("unset fields should stay nil: %+v", a)
	}
	if a.ConcurrencyOrDefault() != DefaultConcurrency {
		t.Fatalf("concurrency default = %d", a.ConcurrencyOrDefault())
	}
	if a.JoinTimeoutOrDefault() != DefaultJoinTimeout {
		t.Fatalf("join_timeout default = %s", a.JoinTimeoutOrDefault())
	}
	if a.IsJoin() {
		t.Fatal("single input with no correlate_on is not a join")
	}
	if a.Output != "" {
		t.Fatal("expected a terminal action")
	}
}

// YAML and JSON are two spellings of one definition; they must decode
// identically, or validation on one path proves nothing about the other.
func TestYAMLAndJSONDecodeIdentically(t *testing.T) {
	const yamlSrc = `
name: close-order
inputs:
  - queue: invoices.approved
    filter: 'payload.total > 0'
actor: subprocess
instructions:
  command: ["./close.sh"]
  timeout: 30s
output: orders.closed
concurrency: 2
`
	const jsonSrc = `{
	  "name": "close-order",
	  "inputs": [{"queue": "invoices.approved", "filter": "payload.total > 0"}],
	  "actor": "subprocess",
	  "instructions": {"timeout": "30s", "command": ["./close.sh"]},
	  "output": "orders.closed",
	  "concurrency": 2
	}`

	fromYAML, err := ParseYAML([]byte(yamlSrc))
	if err != nil {
		t.Fatalf("parse yaml: %v", err)
	}
	fromJSON, err := ParseJSON([]byte(jsonSrc))
	if err != nil {
		t.Fatalf("parse json: %v", err)
	}

	yc, err := fromYAML.Canonical()
	if err != nil {
		t.Fatalf("canonical yaml: %v", err)
	}
	jc, err := fromJSON.Canonical()
	if err != nil {
		t.Fatalf("canonical json: %v", err)
	}
	if string(yc) != string(jc) {
		t.Fatalf("canonical forms differ:\n yaml: %s\n json: %s", yc, jc)
	}
}

// Canonicalization is what makes re-applying an unchanged file a no-op, so it
// must be a fixed point: parse → canonical → parse → canonical is stable, and
// insignificant formatting differences collapse.
func TestCanonicalIsStable(t *testing.T) {
	a, err := ParseYAML([]byte(minimalYAML))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	first, err := a.Canonical()
	if err != nil {
		t.Fatalf("canonical: %v", err)
	}

	b, err := ParseJSON(first)
	if err != nil {
		t.Fatalf("reparse canonical: %v", err)
	}
	second, err := b.Canonical()
	if err != nil {
		t.Fatalf("canonical again: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("canonical form not a fixed point:\n 1: %s\n 2: %s", first, second)
	}

	// Key order and whitespace inside the opaque instructions block must not
	// produce a spurious new version.
	reordered, err := ParseJSON([]byte(`{"actor":"subprocess","instructions":{"command":["./close.sh"]},
	  "inputs":[{"queue":"invoices.approved"}],"name":"close-order"}`))
	if err != nil {
		t.Fatalf("parse reordered: %v", err)
	}
	rc, err := reordered.Canonical()
	if err != nil {
		t.Fatalf("canonical reordered: %v", err)
	}
	if string(rc) != string(first) {
		t.Fatalf("key order changed the canonical form:\n want: %s\n got:  %s", first, rc)
	}
}

func TestDurationRoundTrip(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`"300s"`, "5m0s"},
		{`"24h"`, "24h0m0s"},
		{`"1h30m"`, "1h30m0s"},
	} {
		var d Duration
		if err := json.Unmarshal([]byte(tc.in), &d); err != nil {
			t.Fatalf("unmarshal %s: %v", tc.in, err)
		}
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("marshal %s: %v", tc.in, err)
		}
		if got := string(b); got != `"`+tc.want+`"` {
			t.Fatalf("%s round-tripped to %s, want %q", tc.in, got, tc.want)
		}
	}

	var d Duration
	if err := json.Unmarshal([]byte(`300`), &d); err == nil {
		t.Fatal("a bare number should not decode as a duration")
	}
	if err := json.Unmarshal([]byte(`"soon"`), &d); err == nil {
		t.Fatal("an unparseable duration should error")
	}
}

// A typo'd key must be an error, not a silently ignored setting.
func TestUnknownFieldsRejected(t *testing.T) {
	for name, src := range map[string]string{
		"top level":   "name: x\nconcurency: 2\n",
		"input":       "name: x\ninputs:\n  - queue: q\n    fliter: 'true'\n",
		"instruction": "name: x\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  cmd: [\"./x\"]\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseYAML([]byte(src)); err == nil {
				t.Fatal("expected an error for an unknown field")
			} else if !errors.Is(err, ErrInvalidAction) {
				t.Fatalf("error does not wrap ErrInvalidAction: %v", err)
			}
		})
	}
}

func TestParseYAMLEmpty(t *testing.T) {
	if _, err := ParseYAML(nil); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("empty definition: %v", err)
	}
	if _, err := ParseYAML([]byte("name: [oops\n")); !errors.Is(err, ErrInvalidAction) {
		t.Fatalf("malformed yaml: %v", err)
	}
}

// Every error a user can provoke has to read as one actionable line.
func TestValidationErrorsAreOneLine(t *testing.T) {
	_, err := ParseYAML([]byte("name: x\nactor: rocket\ninputs:\n  - queue: q\n"))
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), "\n") {
		t.Fatalf("multi-line error: %q", err)
	}
}
