package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrInvalidAction marks a definition the system refuses to register. The API
// layer maps it to a 400 with code invalid_action; every error wrapping it is
// a single actionable line naming the offending field.
var ErrInvalidAction = errors.New("invalid action")

// invalidf wraps ErrInvalidAction with the specifics.
func invalidf(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, ErrInvalidAction)...)
}

// nameRE also governs queue names: lowercase alphanumerics, separated by
// dashes, with dots allowed in queues for the `noun.verb` convention.
var (
	nameRE  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	queueRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$`)
)

// Validate checks a decoded definition. It is the single validation entry
// point: both the YAML and the JSON path go through it, and the daemon runs it
// again on whatever the client sends.
func Validate(a *Action) error {
	if a.Name == "" {
		return invalidf("name is required")
	}
	if !nameRE.MatchString(a.Name) {
		return invalidf("invalid name %q: use lowercase letters, digits, and dashes", a.Name)
	}
	if a.Actor == "" {
		return invalidf("action %s: actor is required (one of %s)", a.Name, strings.Join(knownActors, ", "))
	}
	if a.Actor != ActorSubprocess {
		return invalidf("action %s: unknown actor %q: known actors are %s",
			a.Name, a.Actor, strings.Join(knownActors, ", "))
	}
	if err := validateInputs(a); err != nil {
		return err
	}
	if err := validateInstructions(a); err != nil {
		return err
	}
	if a.Output != "" && !queueRE.MatchString(a.Output) {
		return invalidf("action %s: invalid output queue %q: use lowercase letters, digits, dots, and dashes",
			a.Name, a.Output)
	}
	if a.Concurrency != nil && *a.Concurrency < 1 {
		return invalidf("action %s: concurrency must be >= 1, got %d", a.Name, *a.Concurrency)
	}
	if a.MaxAttempts != nil && *a.MaxAttempts < 1 {
		return invalidf("action %s: max_attempts must be >= 1, got %d", a.Name, *a.MaxAttempts)
	}
	if a.JoinTimeout != nil {
		if !a.IsJoin() {
			return invalidf("action %s: join_timeout requires correlate_on on every input", a.Name)
		}
		if *a.JoinTimeout <= 0 {
			return invalidf("action %s: join_timeout must be positive, got %s", a.Name, a.JoinTimeout)
		}
	}

	// Compiling here is what turns a bad expression into a registration-time
	// error instead of a scheduler-time dead-letter.
	if _, err := Compile(a); err != nil {
		return err
	}
	return nil
}

func validateInputs(a *Action) error {
	if len(a.Inputs) == 0 {
		return invalidf("action %s: inputs is required with at least one queue", a.Name)
	}

	seen := make(map[string]struct{}, len(a.Inputs))
	correlated := 0
	for i, in := range a.Inputs {
		if in.Queue == "" {
			return invalidf("action %s: inputs[%d].queue is required", a.Name, i)
		}
		if !queueRE.MatchString(in.Queue) {
			return invalidf("action %s: invalid input queue %q: use lowercase letters, digits, dots, and dashes",
				a.Name, in.Queue)
		}
		if _, dup := seen[in.Queue]; dup {
			return invalidf("action %s: duplicate input queue %q", a.Name, in.Queue)
		}
		seen[in.Queue] = struct{}{}

		// A direct self-loop is almost always a typo. A→B→A stays legal; the
		// hops cap is what bounds real cycles.
		if a.Output != "" && in.Queue == a.Output {
			return invalidf("action %s: input queue %q is also the output queue: a self-loop needs an intermediate queue",
				a.Name, in.Queue)
		}
		if in.CorrelateOn != "" {
			correlated++
		}
	}

	if correlated > 0 && correlated != len(a.Inputs) {
		return invalidf("action %s: correlate_on must be set on every input or none: a join is all-or-nothing (%d of %d set)",
			a.Name, correlated, len(a.Inputs))
	}
	if correlated > 0 && len(a.Inputs) < 2 {
		return invalidf("action %s: correlate_on needs at least 2 inputs to join", a.Name)
	}
	return nil
}

func validateInstructions(a *Action) error {
	switch a.Actor {
	case ActorSubprocess:
		if len(a.Instructions) == 0 {
			return invalidf("action %s: instructions.command is required for actor %s", a.Name, ActorSubprocess)
		}
		si, err := decodeSubprocess(a.Instructions)
		if err != nil {
			return invalidf("action %s: instructions: %v", a.Name, err)
		}
		if len(si.Command) == 0 {
			return invalidf("action %s: instructions.command is required: give it argv form, e.g. [\"./run.sh\"]", a.Name)
		}
		if strings.TrimSpace(si.Command[0]) == "" {
			return invalidf("action %s: instructions.command[0] must name an executable", a.Name)
		}
		if si.Timeout != nil && *si.Timeout <= 0 {
			return invalidf("action %s: instructions.timeout must be positive, got %s", a.Name, si.Timeout)
		}
	}
	return nil
}
