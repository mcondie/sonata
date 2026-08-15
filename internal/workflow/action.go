// Package workflow parses, validates, and compiles action definitions.
//
// YAML (authored on disk) and JSON (ad-hoc, over the wire) decode into the
// same Action struct and run through the same validation — never validate in
// one parser alone, or the other path skips the checks.
package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	yaml "go.yaml.in/yaml/v3"
)

// Actor types this version understands. Adding one is additive: definitions
// naming an unknown actor are rejected at registration.
const (
	ActorSubprocess = "subprocess"
)

// knownActors is the rejection message's source of truth.
var knownActors = []string{ActorSubprocess}

// Defaults applied when a definition leaves the field unset.
const (
	DefaultConcurrency = 1
	DefaultJoinTimeout = 24 * time.Hour
)

// Action is the authored unit: input queues, an actor, and an output queue.
// Pointer fields distinguish "unset" (inherit the default) from an explicit
// value, so an explicit zero is rejected rather than silently defaulted.
type Action struct {
	Name         string          `json:"name"`
	Inputs       []Input         `json:"inputs,omitempty"`
	Actor        string          `json:"actor"`
	Instructions json.RawMessage `json:"instructions"`
	Output       string          `json:"output,omitempty"`
	Concurrency  *int            `json:"concurrency,omitempty"`
	MaxAttempts  *int            `json:"max_attempts,omitempty"`
	JoinTimeout  *Duration       `json:"join_timeout,omitempty"`
}

// Input is one subscribed queue with its optional CEL expressions.
type Input struct {
	Queue string `json:"queue"`
	// Filter is a CEL expression evaluating to bool. Empty accepts everything.
	Filter string `json:"filter,omitempty"`
	// CorrelateOn is a CEL expression yielding the join key. Its presence on
	// every input turns the inputs into a join (matching lands in spec 007).
	CorrelateOn string `json:"correlate_on,omitempty"`
}

// SubprocessInstructions is the `instructions` block for actor: subprocess.
type SubprocessInstructions struct {
	Command []string  `json:"command"`
	Timeout *Duration `json:"timeout,omitempty"`
}

// IsJoin reports whether the action correlates its inputs. Validation
// guarantees correlate_on is all-or-nothing, so the first input decides.
func (a *Action) IsJoin() bool {
	return len(a.Inputs) > 0 && a.Inputs[0].CorrelateOn != ""
}

// Queues returns the input queue names in definition order.
func (a *Action) Queues() []string {
	qs := make([]string, 0, len(a.Inputs))
	for _, in := range a.Inputs {
		qs = append(qs, in.Queue)
	}
	return qs
}

// ConcurrencyOrDefault returns the effective concurrency limit.
func (a *Action) ConcurrencyOrDefault() int {
	if a.Concurrency == nil {
		return DefaultConcurrency
	}
	return *a.Concurrency
}

// JoinTimeoutOrDefault returns the effective join TTL.
func (a *Action) JoinTimeoutOrDefault() time.Duration {
	if a.JoinTimeout == nil {
		return DefaultJoinTimeout
	}
	return time.Duration(*a.JoinTimeout)
}

// Subprocess decodes the instructions block for a subprocess action.
func (a *Action) Subprocess() (*SubprocessInstructions, error) {
	if a.Actor != ActorSubprocess {
		return nil, fmt.Errorf("action %s: actor is %q, not %s", a.Name, a.Actor, ActorSubprocess)
	}
	return decodeSubprocess(a.Instructions)
}

func decodeSubprocess(raw json.RawMessage) (*SubprocessInstructions, error) {
	var si SubprocessInstructions
	if err := strictUnmarshal(raw, &si); err != nil {
		return nil, err
	}
	return &si, nil
}

// ParseJSON decodes and validates a definition in JSON — the wire form, and
// the form the daemon re-validates before storing.
func ParseJSON(data []byte) (*Action, error) {
	var a Action
	if err := strictUnmarshal(data, &a); err != nil {
		return nil, invalidf("%v", err)
	}
	if err := a.normalize(); err != nil {
		return nil, err
	}
	if err := Validate(&a); err != nil {
		return nil, err
	}
	return &a, nil
}

// ParseYAML decodes and validates a definition in YAML — the authoring form.
// It converts to JSON first so both paths share one set of struct tags and one
// strict-unknown-field check.
func ParseYAML(data []byte) (*Action, error) {
	var doc any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, invalidf("parse yaml: %v", err)
	}
	if doc == nil {
		return nil, invalidf("definition is empty")
	}
	j, err := json.Marshal(doc)
	if err != nil {
		return nil, invalidf("parse yaml: %v", err)
	}
	return ParseJSON(j)
}

// normalize rewrites the opaque instructions block into its canonical JSON
// form (object keys sorted, insignificant whitespace dropped) so that two
// definitions differing only in formatting canonicalize identically.
func (a *Action) normalize() error {
	if len(a.Instructions) == 0 || bytes.Equal(a.Instructions, []byte("null")) {
		a.Instructions = nil
		return nil
	}
	var v any
	if err := json.Unmarshal(a.Instructions, &v); err != nil {
		return invalidf("instructions: %v", err)
	}
	norm, err := json.Marshal(v)
	if err != nil {
		return invalidf("instructions: %v", err)
	}
	a.Instructions = norm
	return nil
}

// Canonical returns the stored form of the definition: stable field order,
// normalized instructions, durations as strings. Byte equality of two
// canonical forms is what makes re-applying an unchanged file a no-op.
func (a *Action) Canonical() ([]byte, error) {
	b, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("canonicalize action %s: %w", a.Name, err)
	}
	return b, nil
}

// strictUnmarshal rejects unknown fields, so a typo'd key is an error instead
// of a silently ignored setting.
func strictUnmarshal(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return err
	}
	// Trailing content means the caller sent more than one document.
	if dec.More() {
		return fmt.Errorf("unexpected trailing content after definition")
	}
	return nil
}

// Duration is a time.Duration that reads and writes as a Go duration string
// ("300s", "24h"), which is how both YAML and JSON definitions spell it.
type Duration time.Duration

// UnmarshalJSON accepts a duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\" or \"24h\", got %s", b)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: want a value like \"30s\" or \"24h\"", s)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalJSON writes the canonical duration string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d Duration) String() string { return time.Duration(d).String() }
