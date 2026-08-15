package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Message is a row in the messages table. Messages are immutable facts:
// nothing updates or deletes them outside explicit retention.
type Message struct {
	ID                  string
	Queue               string
	Payload             json.RawMessage
	Headers             json.RawMessage
	TraceID             string
	OriginAction        *string
	OriginActionVersion *int64
	OriginMessageID     *string
	CreatedAt           time.Time
}

// QueueInfo is one row of the derived queue listing.
type QueueInfo struct {
	Name     string
	Messages int64
}

// ListOptions filters ListMessages. Zero values mean "no filter".
type ListOptions struct {
	Queue   string
	TraceID string
	Limit   int    // default and cap applied by the store
	BeforeID string // keyset cursor: only rows with id < BeforeID
}

const (
	defaultListLimit = 50
	maxListLimit     = 1000
)

// AppendMessage inserts a fully populated message. The caller (the API
// handler) owns stamping — the store stores what it is given.
func (s *Store) AppendMessage(ctx context.Context, m *Message) error {
	return s.write(ctx, func(tx *sql.Tx) error {
		return insertMessage(ctx, tx, m)
	})
}

// insertMessage is split out so future callers (the executor's transactional
// outbox) can append inside a larger write transaction.
func insertMessage(ctx context.Context, tx *sql.Tx, m *Message) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO messages
			(id, queue, payload, headers, trace_id,
			 origin_action, origin_action_version, origin_message_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.Queue, string(m.Payload), string(m.Headers), m.TraceID,
		m.OriginAction, m.OriginActionVersion, m.OriginMessageID,
		m.CreatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("insert message: %w", err)
	}
	return nil
}

// GetMessage returns one message or ErrNotFound.
func (s *Store) GetMessage(ctx context.Context, id string) (*Message, error) {
	row := s.r.QueryRowContext(ctx, `
		SELECT id, queue, payload, headers, trace_id,
		       origin_action, origin_action_version, origin_message_id, created_at
		FROM messages WHERE id = ?`, id)
	m, err := scanMessage(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("message %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get message: %w", err)
	}
	return m, nil
}

// ListMessages returns messages newest-first, filtered by opts. Pagination is
// keyset on id: pass the last id of a page as BeforeID to get the next.
func (s *Store) ListMessages(ctx context.Context, opts ListOptions) ([]*Message, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}

	q := `
		SELECT id, queue, payload, headers, trace_id,
		       origin_action, origin_action_version, origin_message_id, created_at
		FROM messages WHERE 1=1`
	args := []any{}
	if opts.Queue != "" {
		q += ` AND queue = ?`
		args = append(args, opts.Queue)
	}
	if opts.TraceID != "" {
		q += ` AND trace_id = ?`
		args = append(args, opts.TraceID)
	}
	if opts.BeforeID != "" {
		q += ` AND id < ?`
		args = append(args, opts.BeforeID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	defer rows.Close()

	var out []*Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("list messages: %w", err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list messages: %w", err)
	}
	return out, nil
}

// ListQueues derives the set of known queues from the messages table.
func (s *Store) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	rows, err := s.r.QueryContext(ctx,
		`SELECT queue, COUNT(*) FROM messages GROUP BY queue ORDER BY queue`)
	if err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	defer rows.Close()

	var out []QueueInfo
	for rows.Next() {
		var q QueueInfo
		if err := rows.Scan(&q.Name, &q.Messages); err != nil {
			return nil, fmt.Errorf("list queues: %w", err)
		}
		out = append(out, q)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list queues: %w", err)
	}
	return out, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanMessage(row scanner) (*Message, error) {
	var (
		m       Message
		payload string
		headers string
		created string
	)
	err := row.Scan(&m.ID, &m.Queue, &payload, &headers, &m.TraceID,
		&m.OriginAction, &m.OriginActionVersion, &m.OriginMessageID, &created)
	if err != nil {
		return nil, err
	}
	m.Payload = json.RawMessage(payload)
	m.Headers = json.RawMessage(headers)
	if m.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return nil, fmt.Errorf("parse created_at %q: %w", created, err)
	}
	return &m, nil
}
