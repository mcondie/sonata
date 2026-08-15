package workflow

import (
	"errors"
	"strings"
	"testing"
)

// One case per row of the reject table in spec 005, plus the accept cases.
// wantErr is a substring the message must carry, so an error stays actionable
// rather than merely non-nil.
func TestValidateRejects(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing name",
			yaml:    "inputs:\n  - queue: q\nactor: subprocess\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "name is required",
		},
		{
			name:    "invalid name",
			yaml:    "name: Close_Order\ninputs:\n  - queue: q\nactor: subprocess\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "invalid name",
		},
		{
			name:    "missing actor",
			yaml:    "name: a\ninputs:\n  - queue: q\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "actor is required",
		},
		{
			name:    "unknown actor",
			yaml:    "name: a\nactor: rocket\ninputs:\n  - queue: q\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: `unknown actor "rocket": known actors are subprocess`,
		},
		{
			name:    "missing inputs",
			yaml:    "name: a\nactor: subprocess\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "inputs is required",
		},
		{
			name:    "empty input queue",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - filter: 'true'\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "inputs[0].queue is required",
		},
		{
			name:    "missing instructions",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\n",
			wantErr: "instructions.command is required",
		},
		{
			name:    "missing command",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  timeout: 30s\n",
			wantErr: "instructions.command is required",
		},
		{
			name:    "empty command executable",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  command: [\"\"]\n",
			wantErr: "instructions.command[0] must name an executable",
		},
		{
			name:    "duplicate input queue",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: dup.q\n  - queue: dup.q\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: `duplicate input queue "dup.q"`,
		},
		{
			name: "correlate_on on some inputs only",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'payload.k'\n" +
				"  - queue: q2\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "correlate_on must be set on every input or none",
		},
		{
			name: "correlate_on with a single input",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'payload.k'\n" +
				"instructions:\n  command: [\"./x\"]\n",
			wantErr: "correlate_on needs at least 2 inputs",
		},
		{
			name: "join_timeout without correlate_on",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q\njoin_timeout: 1h\n" +
				"instructions:\n  command: [\"./x\"]\n",
			wantErr: "join_timeout requires correlate_on",
		},
		{
			name: "uncompilable filter",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q\n    filter: 'payload.total >'\n" +
				"instructions:\n  command: [\"./x\"]\n",
			wantErr: "filter",
		},
		{
			name: "filter is not a bool",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q\n    filter: '\"yes\"'\n" +
				"instructions:\n  command: [\"./x\"]\n",
			wantErr: "must evaluate to bool",
		},
		{
			name: "uncompilable correlate_on",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'nope(1)'\n" +
				"  - queue: q2\n    correlate_on: 'payload.k'\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "correlate_on",
		},
		{
			name: "correlate_on is not a string or int",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'true'\n" +
				"  - queue: q2\n    correlate_on: 'payload.k'\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "must evaluate to string or int",
		},
		{
			name:    "concurrency below one",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\nconcurrency: 0\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "concurrency must be >= 1",
		},
		{
			name:    "max_attempts below one",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\nmax_attempts: -2\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "max_attempts must be >= 1",
		},
		{
			name: "input queue equals output",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: loop.q\noutput: loop.q\n" +
				"instructions:\n  command: [\"./x\"]\n",
			wantErr: "self-loop needs an intermediate queue",
		},
		{
			name:    "invalid output queue",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\noutput: Orders Closed\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "invalid output queue",
		},
		{
			name:    "non-positive timeout",
			yaml:    "name: a\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  command: [\"./x\"]\n  timeout: 0s\n",
			wantErr: "instructions.timeout must be positive",
		},
		{
			name: "non-positive join_timeout",
			yaml: "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'payload.k'\n" +
				"  - queue: q2\n    correlate_on: 'payload.k'\njoin_timeout: -1h\ninstructions:\n  command: [\"./x\"]\n",
			wantErr: "join_timeout must be positive",
		},
	}

	seen := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseYAML([]byte(tc.yaml))
			if err == nil {
				t.Fatal("expected a validation error")
			}
			if !errors.Is(err, ErrInvalidAction) {
				t.Fatalf("error does not wrap ErrInvalidAction: %v", err)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err, tc.wantErr)
			}
			if strings.Contains(err.Error(), "\n") {
				t.Fatalf("error is not one line: %q", err)
			}
			// Distinct rejection reasons must produce distinct messages, or a
			// user cannot tell which rule they broke.
			if prev, dup := seen[err.Error()]; dup {
				t.Fatalf("error message %q is shared with case %q", err, prev)
			}
			seen[err.Error()] = tc.name
		})
	}
}

func TestValidateAccepts(t *testing.T) {
	cases := map[string]string{
		"minimal": "name: a\nactor: subprocess\ninputs:\n  - queue: q\ninstructions:\n  command: [\"./x\"]\n",
		"terminal action with no output": "name: a\nactor: subprocess\ninputs:\n  - queue: q\n    filter: 'payload.n > 0'\n" +
			"instructions:\n  command: [\"./x\"]\n",
		"full join": "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'payload.k'\n" +
			"    filter: 'payload.n > 0'\n  - queue: q2\n    correlate_on: 'string(payload.k)'\n" +
			"output: out.q\njoin_timeout: 12h\nconcurrency: 4\nmax_attempts: 3\n" +
			"instructions:\n  command: [\"./x\", \"-v\"]\n  timeout: 300s\n",
		"int correlation key": "name: a\nactor: subprocess\ninputs:\n  - queue: q1\n    correlate_on: 'payload.n'\n" +
			"  - queue: q2\n    correlate_on: '7'\ninstructions:\n  command: [\"./x\"]\n",
		"headers and metadata in expressions": "name: a\nactor: subprocess\ninputs:\n  - queue: q\n" +
			"    filter: 'queue == \"q\" && trace_id != \"\" && headers.hops < 10'\ninstructions:\n  command: [\"./x\"]\n",
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseYAML([]byte(src)); err != nil {
				t.Fatalf("expected acceptance, got: %v", err)
			}
		})
	}
}
