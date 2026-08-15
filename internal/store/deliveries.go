package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Delivery states. pending → claimed → done | failed (retryable) | filtered |
// dead; any non-terminal state → cancelled when its action is disabled or the
// claiming version no longer covers it.
const (
	StatePending   = "pending"
	StateClaimed   = "claimed"
	StateDone      = "done"
	StateFailed    = "failed"
	StateFiltered  = "filtered"
	StateDead      = "dead"
	StateCancelled = "cancelled"
)

// ErrNotDead is returned by ReplayDelivery for deliveries in any other state.
// The API layer maps it to 409.
var ErrNotDead = errors.New("delivery is not dead")

// Delivery is one row of per-(message × action) processing state.
type Delivery struct {
	ID            string
	MessageID     *string
	ActionName    string
	ActionVersion *int64
	State         string
	Attempt       int
	NotBefore     *time.Time
	Pgid          *int
	StderrTail    *string
	Error         *string
	ClaimedAt     *time.Time
	CompletedAt   *time.Time
}

// Work is a claimable delivery joined with its message.
type Work struct {
	Delivery Delivery
	Message  Message
}

// DeliveryListOptions filters ListDeliveries. Zero values mean "no filter".
type DeliveryListOptions struct {
	Action    string
	State     string
	MessageID string
	Limit     int
	BeforeID  string
}

// subscription is the slice of an action definition materialization needs.
// The store otherwise treats definitions as opaque, but delivery fan-out must
// be transactional with the message insert, so the decode happens here.
type subscription struct {
	Inputs []struct {
		Queue       string `json:"queue"`
		CorrelateOn string `json:"correlate_on"`
	} `json:"inputs"`
}

// materializeDeliveries inserts one pending delivery per enabled, non-join
// action subscribed to the message's queue, inside the caller's transaction.
// Join actions are skipped until spec 007 activates them.
func materializeDeliveries(ctx context.Context, tx *sql.Tx, m *Message) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT a.name, a.definition
		FROM actions a
		JOIN (SELECT name, MAX(version) AS version FROM actions GROUP BY name) cur
		  ON cur.name = a.name AND cur.version = a.version
		WHERE a.enabled
		ORDER BY a.name`)
	if err != nil {
		return 0, fmt.Errorf("load subscribers: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var (
			name string
			def  string
		)
		if err := rows.Scan(&name, &def); err != nil {
			return 0, fmt.Errorf("scan subscriber: %w", err)
		}
		var sub subscription
		if err := json.Unmarshal([]byte(def), &sub); err != nil {
			return 0, fmt.Errorf("decode definition of %s: %w", name, err)
		}
		for _, in := range sub.Inputs {
			if in.CorrelateOn != "" {
				break // join action: no eager deliveries in this slice
			}
			if in.Queue == m.Queue {
				names = append(names, name)
				break
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("load subscribers: %w", err)
	}

	for _, name := range names {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO deliveries (id, message_id, action_name, state)
			VALUES (?, ?, ?, ?)`,
			NewID(), m.ID, name, StatePending); err != nil {
			return 0, fmt.Errorf("materialize delivery for %s: %w", name, err)
		}
	}
	return len(names), nil
}

// NextEligible returns the oldest claimable delivery for an action: pending,
// or failed with its backoff gate passed. Returns ErrNotFound when none.
func (s *Store) NextEligible(ctx context.Context, action string, now time.Time) (*Work, error) {
	row := s.r.QueryRowContext(ctx, `
		SELECT d.id, d.message_id, d.action_name, d.action_version, d.state,
		       d.attempt, d.not_before, d.pgid, d.stderr_tail, d.error,
		       d.claimed_at, d.completed_at,
		       m.id, m.queue, m.payload, m.headers, m.trace_id,
		       m.origin_action, m.origin_action_version, m.origin_message_id, m.created_at
		FROM deliveries d JOIN messages m ON m.id = d.message_id
		WHERE d.action_name = ?
		  AND (d.state = ? OR (d.state = ? AND d.not_before <= ?))
		ORDER BY d.id LIMIT 1`,
		action, StatePending, StateFailed, now.UTC().Format(time.RFC3339Nano))

	var (
		w         Work
		notBefore sql.NullString
		claimedAt sql.NullString
		completed sql.NullString
		payload   string
		headers   string
		created   string
	)
	err := row.Scan(&w.Delivery.ID, &w.Delivery.MessageID, &w.Delivery.ActionName,
		&w.Delivery.ActionVersion, &w.Delivery.State, &w.Delivery.Attempt,
		&notBefore, &w.Delivery.Pgid, &w.Delivery.StderrTail, &w.Delivery.Error,
		&claimedAt, &completed,
		&w.Message.ID, &w.Message.Queue, &payload, &headers, &w.Message.TraceID,
		&w.Message.OriginAction, &w.Message.OriginActionVersion,
		&w.Message.OriginMessageID, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("next eligible for %s: %w", action, err)
	}
	w.Message.Payload = json.RawMessage(payload)
	w.Message.Headers = json.RawMessage(headers)
	if w.Message.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	if w.Delivery.NotBefore, err = nullTime(notBefore); err != nil {
		return nil, err
	}
	return &w, nil
}

// ClaimDelivery moves an eligible delivery to claimed, pinning the action
// version it will execute under and counting the attempt.
func (s *Store) ClaimDelivery(ctx context.Context, id string, version int64, now time.Time) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, action_version = ?, attempt = attempt + 1,
			    claimed_at = ?, not_before = NULL
			WHERE id = ? AND state IN (?, ?)`,
			StateClaimed, version, now.UTC().Format(time.RFC3339Nano),
			id, StatePending, StateFailed)
		if err != nil {
			return fmt.Errorf("claim delivery %s: %w", id, err)
		}
		return mustAffect(res, "claim", id)
	})
}

// ResolveDelivery moves a delivery to a terminal state without an execution:
// filtered, dead (hop cap, broken filter), or cancelled (superseded).
func (s *Store) ResolveDelivery(ctx context.Context, id, state, errMsg string, now time.Time) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, error = NULLIF(?, ''), completed_at = ?, pgid = NULL
			WHERE id = ?`,
			state, errMsg, now.UTC().Format(time.RFC3339Nano), id)
		if err != nil {
			return fmt.Errorf("resolve delivery %s: %w", id, err)
		}
		return mustAffect(res, "resolve", id)
	})
}

// SetDeliveryPgid records the live process group of a claimed delivery, so a
// daemon crash leaves enough behind for the next daemon to reap it.
func (s *Store) SetDeliveryPgid(ctx context.Context, id string, pgid int) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`UPDATE deliveries SET pgid = ? WHERE id = ? AND state = ?`,
			pgid, id, StateClaimed)
		if err != nil {
			return fmt.Errorf("set pgid on %s: %w", id, err)
		}
		return nil
	})
}

// CompleteDelivery is the transactional outbox: mark the delivery done and
// append its output messages — with their own eager deliveries — in one
// transaction. A crash can never produce consumed-but-unemitted or
// emitted-twice. Returns how many downstream deliveries were created.
func (s *Store) CompleteDelivery(ctx context.Context, id, stderrTail string, outputs []*Message, now time.Time) (int, error) {
	created := 0
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, stderr_tail = NULLIF(?, ''), completed_at = ?, pgid = NULL
			WHERE id = ? AND state = ?`,
			StateDone, stderrTail, now.UTC().Format(time.RFC3339Nano), id, StateClaimed)
		if err != nil {
			return fmt.Errorf("complete delivery %s: %w", id, err)
		}
		if err := mustAffect(res, "complete", id); err != nil {
			return err
		}
		for _, m := range outputs {
			if err := insertMessage(ctx, tx, m); err != nil {
				return err
			}
			n, err := materializeDeliveries(ctx, tx, m)
			if err != nil {
				return err
			}
			created += n
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return created, nil
}

// FailDelivery records a failed execution: dead when the attempt budget is
// spent, otherwise failed with its backoff gate.
func (s *Store) FailDelivery(ctx context.Context, id string, dead bool, notBefore time.Time, errMsg, stderrTail string, now time.Time) error {
	state := StateFailed
	var gate any
	if dead {
		state = StateDead
	} else {
		gate = notBefore.UTC().Format(time.RFC3339Nano)
	}
	return s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, not_before = ?, error = ?, stderr_tail = NULLIF(?, ''),
			    completed_at = ?, pgid = NULL
			WHERE id = ? AND state = ?`,
			state, gate, errMsg, stderrTail,
			now.UTC().Format(time.RFC3339Nano), id, StateClaimed)
		if err != nil {
			return fmt.Errorf("fail delivery %s: %w", id, err)
		}
		return mustAffect(res, "fail", id)
	})
}

// ReplayDelivery resets a dead delivery to pending under attempt 0. It will
// claim under the current action version — the point of replaying after a fix.
func (s *Store) ReplayDelivery(ctx context.Context, id string) (*Delivery, error) {
	var d *Delivery
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, attempt = 0, error = NULL, stderr_tail = NULL,
			    not_before = NULL, action_version = NULL,
			    claimed_at = NULL, completed_at = NULL
			WHERE id = ? AND state = ?`,
			StatePending, id, StateDead)
		if err != nil {
			return fmt.Errorf("replay delivery %s: %w", id, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			// Distinguish missing from wrong-state for the error contract.
			var state string
			err := tx.QueryRowContext(ctx,
				`SELECT state FROM deliveries WHERE id = ?`, id).Scan(&state)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("delivery %s: %w", id, ErrNotFound)
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("delivery %s is %s: %w", id, state, ErrNotDead)
		}
		d, err = getDelivery(ctx, tx, id)
		return err
	})
	if err != nil {
		return nil, err
	}
	return d, nil
}

// CancelDeliveries moves every pending and retry-waiting delivery of an
// action to cancelled — disable must not strand non-terminal rows that block
// idle-out and prune. Claimed executions are left to finish.
func (s *Store) CancelDeliveries(ctx context.Context, action, reason string, now time.Time) (int, error) {
	var n int64
	err := s.write(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, error = ?, not_before = NULL, completed_at = ?
			WHERE action_name = ? AND state IN (?, ?)`,
			StateCancelled, reason, now.UTC().Format(time.RFC3339Nano),
			action, StatePending, StateFailed)
		if err != nil {
			return fmt.Errorf("cancel deliveries of %s: %w", action, err)
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, err
	}
	return int(n), nil
}

// ClaimedDeliveries returns every delivery a previous daemon left claimed —
// the orphan-reap input.
func (s *Store) ClaimedDeliveries(ctx context.Context) ([]*Delivery, error) {
	return s.listDeliveries(ctx, `
		SELECT `+deliveryCols+` FROM deliveries WHERE state = ? ORDER BY id`,
		StateClaimed)
}

// ResetOrphan returns a claimed delivery from a dead daemon to the retry
// path: failed, claimable immediately.
func (s *Store) ResetOrphan(ctx context.Context, id string, now time.Time) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			UPDATE deliveries
			SET state = ?, not_before = ?, error = ?, pgid = NULL
			WHERE id = ? AND state = ?`,
			StateFailed, now.UTC().Format(time.RFC3339Nano), "daemon restarted",
			id, StateClaimed)
		if err != nil {
			return fmt.Errorf("reset orphan %s: %w", id, err)
		}
		return nil
	})
}

// CountActiveDeliveries counts rows in the states that must keep the daemon
// alive: pending, awaiting retry, or executing. Seeds the scheduler's busy
// counter at startup.
func (s *Store) CountActiveDeliveries(ctx context.Context) (int, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM deliveries WHERE state IN (?, ?, ?)`,
		StatePending, StateFailed, StateClaimed).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count active deliveries: %w", err)
	}
	return n, nil
}

// NextRetryAt returns the earliest backoff gate among failed deliveries, or
// ErrNotFound when nothing is waiting to retry.
func (s *Store) NextRetryAt(ctx context.Context) (time.Time, error) {
	var gate sql.NullString
	err := s.r.QueryRowContext(ctx,
		`SELECT MIN(not_before) FROM deliveries WHERE state = ?`, StateFailed).Scan(&gate)
	if err != nil {
		return time.Time{}, fmt.Errorf("next retry: %w", err)
	}
	if !gate.Valid {
		return time.Time{}, ErrNotFound
	}
	t, err := time.Parse(time.RFC3339Nano, gate.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse not_before %q: %w", gate.String, err)
	}
	return t, nil
}

// GetDelivery returns one delivery or ErrNotFound.
func (s *Store) GetDelivery(ctx context.Context, id string) (*Delivery, error) {
	row := s.r.QueryRowContext(ctx,
		`SELECT `+deliveryCols+` FROM deliveries WHERE id = ?`, id)
	d, err := scanDelivery(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("delivery %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get delivery: %w", err)
	}
	return d, nil
}

// ListDeliveries returns deliveries newest-first, filtered by opts.
func (s *Store) ListDeliveries(ctx context.Context, opts DeliveryListOptions) ([]*Delivery, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	q := `SELECT ` + deliveryCols + ` FROM deliveries WHERE 1=1`
	args := []any{}
	if opts.Action != "" {
		q += ` AND action_name = ?`
		args = append(args, opts.Action)
	}
	if opts.State != "" {
		q += ` AND state = ?`
		args = append(args, opts.State)
	}
	if opts.MessageID != "" {
		q += ` AND message_id = ?`
		args = append(args, opts.MessageID)
	}
	if opts.BeforeID != "" {
		q += ` AND id < ?`
		args = append(args, opts.BeforeID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	return s.listDeliveries(ctx, q, args...)
}

const deliveryCols = `id, message_id, action_name, action_version, state,
	attempt, not_before, pgid, stderr_tail, error, claimed_at, completed_at`

func (s *Store) listDeliveries(ctx context.Context, q string, args ...any) ([]*Delivery, error) {
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	var out []*Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("list deliveries: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	return out, nil
}

func getDelivery(ctx context.Context, tx *sql.Tx, id string) (*Delivery, error) {
	return scanDelivery(tx.QueryRowContext(ctx,
		`SELECT `+deliveryCols+` FROM deliveries WHERE id = ?`, id))
}

func scanDelivery(row scanner) (*Delivery, error) {
	var (
		d         Delivery
		notBefore sql.NullString
		claimedAt sql.NullString
		completed sql.NullString
	)
	err := row.Scan(&d.ID, &d.MessageID, &d.ActionName, &d.ActionVersion,
		&d.State, &d.Attempt, &notBefore, &d.Pgid, &d.StderrTail, &d.Error,
		&claimedAt, &completed)
	if err != nil {
		return nil, err
	}
	if d.NotBefore, err = nullTime(notBefore); err != nil {
		return nil, err
	}
	if d.ClaimedAt, err = nullTime(claimedAt); err != nil {
		return nil, err
	}
	if d.CompletedAt, err = nullTime(completed); err != nil {
		return nil, err
	}
	return &d, nil
}

func nullTime(s sql.NullString) (*time.Time, error) {
	if !s.Valid {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s.String)
	if err != nil {
		return nil, fmt.Errorf("parse time %q: %w", s.String, err)
	}
	return &t, nil
}

// mustAffect turns a zero-row UPDATE into an error: the guarded state was not
// what the caller believed, which for single-owner transitions is a bug.
func mustAffect(res sql.Result, op, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%s delivery %s: row missing or in unexpected state", op, id)
	}
	return nil
}
