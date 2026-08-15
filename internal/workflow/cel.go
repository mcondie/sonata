package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// CEL variable names available to filters and correlate_on expressions.
const (
	VarPayload = "payload"
	VarHeaders = "headers"
	VarQueue   = "queue"
	VarTraceID = "trace_id"
)

// celEnv is built once: environment construction is not cheap and the
// environment is immutable.
var celEnv = sync.OnceValues(func() (*cel.Env, error) {
	return cel.NewEnv(
		cel.Variable(VarPayload, cel.DynType),
		cel.Variable(VarHeaders, cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable(VarQueue, cel.StringType),
		cel.Variable(VarTraceID, cel.StringType),
	)
})

// Env returns the shared CEL environment for action expressions.
func Env() (*cel.Env, error) {
	env, err := celEnv()
	if err != nil {
		return nil, fmt.Errorf("build CEL environment: %w", err)
	}
	return env, nil
}

// Compiled holds an action's compiled expressions, indexed by input position.
// A nil program means the input has no expression of that kind.
type Compiled struct {
	Filters   []cel.Program
	Correlate []cel.Program
}

// Compile compiles every expression in the definition. Type errors CEL can see
// statically (a filter that is not a bool, a correlate_on that is not a string
// or int) are caught here; dyn-typed expressions are re-checked at eval.
func Compile(a *Action) (*Compiled, error) {
	env, err := Env()
	if err != nil {
		return nil, err
	}

	c := &Compiled{
		Filters:   make([]cel.Program, len(a.Inputs)),
		Correlate: make([]cel.Program, len(a.Inputs)),
	}
	for i, in := range a.Inputs {
		if in.Filter != "" {
			prg, err := compileTyped(env, in.Filter, cel.BoolType)
			if err != nil {
				return nil, invalidf("action %s: inputs[%d].filter %q: %v", a.Name, i, in.Filter, err)
			}
			c.Filters[i] = prg
		}
		if in.CorrelateOn != "" {
			prg, err := compileTyped(env, in.CorrelateOn, cel.StringType, cel.IntType)
			if err != nil {
				return nil, invalidf("action %s: inputs[%d].correlate_on %q: %v", a.Name, i, in.CorrelateOn, err)
			}
			c.Correlate[i] = prg
		}
	}
	return c, nil
}

// compileTyped compiles src and, where CEL's checker knows the result type,
// requires it to be one of want.
func compileTyped(env *cel.Env, src string, want ...*cel.Type) (cel.Program, error) {
	ast, iss := env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, flattenCELError(iss.Err())
	}
	if got := ast.OutputType(); got != nil && !got.IsAssignableType(cel.DynType) {
		ok := false
		for _, w := range want {
			if got.IsExactType(w) {
				ok = true
				break
			}
		}
		if !ok {
			return nil, fmt.Errorf("expression must evaluate to %s, got %s", typeList(want), got)
		}
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return prg, nil
}

// flattenCELError reduces a CEL issue to one line. cel-go renders errors with
// a source excerpt and a caret line under it, which is unreadable once wrapped
// into an API error message; the diagnostic itself carries the position.
func flattenCELError(err error) error {
	var kept []string
	for _, line := range strings.Split(err.Error(), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "|") {
			continue
		}
		kept = append(kept, strings.TrimPrefix(line, "ERROR: <input>:"))
	}
	if len(kept) == 0 {
		return err
	}
	return errors.New(strings.Join(kept, "; "))
}

func typeList(ts []*cel.Type) string {
	out := ""
	for i, t := range ts {
		switch {
		case i == 0:
			out = t.String()
		case i == len(ts)-1:
			out += " or " + t.String()
		default:
			out += ", " + t.String()
		}
	}
	return out
}

// Activation is the evaluation input for one message.
type Activation struct {
	Payload any
	Headers map[string]any
	Queue   string
	TraceID string
}

// NewActivation builds an activation from raw JSON payload and headers.
func NewActivation(payload, headers json.RawMessage, queue, traceID string) (*Activation, error) {
	act := &Activation{Queue: queue, TraceID: traceID, Headers: map[string]any{}}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &act.Payload); err != nil {
			return nil, fmt.Errorf("decode payload: %w", err)
		}
	}
	if len(headers) > 0 {
		if err := json.Unmarshal(headers, &act.Headers); err != nil {
			return nil, fmt.Errorf("decode headers: %w", err)
		}
	}
	return act, nil
}

func (a *Activation) vars() map[string]any {
	return map[string]any{
		VarPayload: a.Payload,
		VarHeaders: a.Headers,
		VarQueue:   a.Queue,
		VarTraceID: a.TraceID,
	}
}

// EvalFilter evaluates a compiled filter. A nil program accepts everything.
func EvalFilter(prg cel.Program, act *Activation) (bool, error) {
	if prg == nil {
		return true, nil
	}
	out, _, err := prg.Eval(act.vars())
	if err != nil {
		return false, fmt.Errorf("evaluate filter: %w", err)
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("filter evaluated to %s, want bool", out.Type())
	}
	return b, nil
}

// EvalCorrelate evaluates a compiled correlate_on expression and coerces the
// result to the correlation key. Only strings and ints are keys; anything else
// is an error the caller dead-letters.
func EvalCorrelate(prg cel.Program, act *Activation) (string, error) {
	if prg == nil {
		return "", fmt.Errorf("no correlate_on expression")
	}
	out, _, err := prg.Eval(act.vars())
	if err != nil {
		return "", fmt.Errorf("evaluate correlate_on: %w", err)
	}
	return correlationKey(out)
}

func correlationKey(out ref.Val) (string, error) {
	switch v := out.(type) {
	case types.String:
		return string(v), nil
	case types.Int:
		return strconv.FormatInt(int64(v), 10), nil
	case types.Uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case types.Double:
		// JSON has no integer type, so a payload id decodes as a double.
		// Integral values are keys; a fractional one is not something two
		// producers can be expected to spell identically.
		f := float64(v)
		if f != math.Trunc(f) || math.IsInf(f, 0) || math.IsNaN(f) {
			return "", fmt.Errorf("correlate_on evaluated to the non-integral number %v, want string or int", f)
		}
		return strconv.FormatInt(int64(f), 10), nil
	}
	return "", fmt.Errorf("correlate_on evaluated to %s, want string or int", out.Type())
}

// Cache holds compiled programs per (action name, version). Action versions
// are immutable, so an entry is valid forever once built.
type Cache struct {
	mu      sync.RWMutex
	entries map[cacheKey]*Compiled
}

type cacheKey struct {
	name    string
	version int64
}

// NewCache returns an empty program cache.
func NewCache() *Cache { return &Cache{entries: map[cacheKey]*Compiled{}} }

// Get returns the compiled programs for a stored action version, compiling on
// first use. Definitions come from the database, which only ever holds
// validated definitions, so a compile error here is corruption, not user input.
func (c *Cache) Get(name string, version int64, a *Action) (*Compiled, error) {
	k := cacheKey{name: name, version: version}

	c.mu.RLock()
	got, ok := c.entries[k]
	c.mu.RUnlock()
	if ok {
		return got, nil
	}

	compiled, err := Compile(a)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Another goroutine may have won the race; either value is equivalent, so
	// keep the stored one and let this one go.
	if got, ok := c.entries[k]; ok {
		return got, nil
	}
	c.entries[k] = compiled
	return compiled, nil
}

// Len reports how many versions are cached.
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
