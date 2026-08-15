package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Action is one immutable version row of a registered action. Definition is
// the canonical JSON the API layer produced; the store treats it as opaque
// bytes and never reinterprets it.
type Action struct {
	Name       string
	Version    int64
	Definition json.RawMessage
	Enabled    bool
	CreatedAt  time.Time
}

// LatestVersion is the sentinel for "whichever version is current".
const LatestVersion int64 = 0

// ApplyAction registers a definition as the next version of name.
//
// Applying a definition byte-identical to the current version is a no-op that
// reports the existing version, so re-applying a directory of files is
// idempotent. A new version inherits the current version's enabled flag —
// editing a disabled action must never silently re-enable it.
func (s *Store) ApplyAction(ctx context.Context, name string, definition json.RawMessage) (a *Action, changed bool, err error) {
	err = s.write(ctx, func(tx *sql.Tx) error {
		// Read inside the write transaction: BEGIN IMMEDIATE already holds the
		// write lock, so concurrent applies of the same name serialize here and
		// versions come out dense and distinct.
		cur, err := scanAction(tx.QueryRowContext(ctx, `
			SELECT name, version, definition, enabled, created_at FROM actions
			WHERE name = ? ORDER BY version DESC LIMIT 1`, name))
		switch {
		case errors.Is(err, sql.ErrNoRows):
			cur = &Action{Name: name, Version: 0, Enabled: true}
		case err != nil:
			return fmt.Errorf("read current action %s: %w", name, err)
		case string(cur.Definition) == string(definition):
			a = cur
			return nil
		}

		next := &Action{
			Name:       name,
			Version:    cur.Version + 1,
			Definition: definition,
			Enabled:    cur.Enabled,
			CreatedAt:  time.Now().UTC(),
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO actions (name, version, definition, enabled, created_at)
			VALUES (?, ?, ?, ?, ?)`,
			next.Name, next.Version, string(next.Definition), next.Enabled,
			next.CreatedAt.Format(time.RFC3339Nano))
		if err != nil {
			return fmt.Errorf("insert action %s: %w", name, err)
		}
		a, changed = next, true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return a, changed, nil
}

// GetAction returns one action version, or the current one when version is
// LatestVersion. Missing rows come back as ErrNotFound.
func (s *Store) GetAction(ctx context.Context, name string, version int64) (*Action, error) {
	q := `SELECT name, version, definition, enabled, created_at FROM actions WHERE name = ?`
	args := []any{name}
	if version == LatestVersion {
		q += ` ORDER BY version DESC LIMIT 1`
	} else {
		q += ` AND version = ?`
		args = append(args, version)
	}

	a, err := scanAction(s.r.QueryRowContext(ctx, q, args...))
	if errors.Is(err, sql.ErrNoRows) {
		if version == LatestVersion {
			return nil, fmt.Errorf("action %s: %w", name, ErrNotFound)
		}
		return nil, fmt.Errorf("action %s version %d: %w", name, version, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get action: %w", err)
	}
	return a, nil
}

// ListActions returns the current version of every registered action, by name.
func (s *Store) ListActions(ctx context.Context) ([]*Action, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT a.name, a.version, a.definition, a.enabled, a.created_at
		FROM actions a
		JOIN (SELECT name, MAX(version) AS version FROM actions GROUP BY name) cur
		  ON cur.name = a.name AND cur.version = a.version
		ORDER BY a.name`)
	if err != nil {
		return nil, fmt.Errorf("list actions: %w", err)
	}
	defer rows.Close()

	var out []*Action
	for rows.Next() {
		a, err := scanAction(rows)
		if err != nil {
			return nil, fmt.Errorf("list actions: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list actions: %w", err)
	}
	return out, nil
}

// ListActionVersions returns every stored version of one action, newest first.
func (s *Store) ListActionVersions(ctx context.Context, name string) ([]*Action, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT name, version, definition, enabled, created_at
		FROM actions WHERE name = ? ORDER BY version DESC`, name)
	if err != nil {
		return nil, fmt.Errorf("list action versions: %w", err)
	}
	defer rows.Close()

	var out []*Action
	for rows.Next() {
		a, err := scanAction(rows)
		if err != nil {
			return nil, fmt.Errorf("list action versions: %w", err)
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list action versions: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("action %s: %w", name, ErrNotFound)
	}
	return out, nil
}

// SetActionEnabled flips the enabled flag on the action's current version,
// which is the row that governs. Returns the updated current version.
func (s *Store) SetActionEnabled(ctx context.Context, name string, enabled bool) (*Action, error) {
	var a *Action
	err := s.write(ctx, func(tx *sql.Tx) error {
		// MAX over no rows yields one NULL row, not zero rows.
		var version sql.NullInt64
		err := tx.QueryRowContext(ctx,
			`SELECT MAX(version) FROM actions WHERE name = ?`, name).Scan(&version)
		if err != nil {
			return fmt.Errorf("read current action %s: %w", name, err)
		}
		if !version.Valid {
			return fmt.Errorf("action %s: %w", name, ErrNotFound)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE actions SET enabled = ? WHERE name = ? AND version = ?`,
			enabled, name, version.Int64); err != nil {
			return fmt.Errorf("set enabled on %s: %w", name, err)
		}
		a, err = scanAction(tx.QueryRowContext(ctx, `
			SELECT name, version, definition, enabled, created_at
			FROM actions WHERE name = ? AND version = ?`, name, version.Int64))
		if err != nil {
			return fmt.Errorf("read back action %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return a, nil
}

func scanAction(row scanner) (*Action, error) {
	var (
		a       Action
		def     string
		created string
	)
	if err := row.Scan(&a.Name, &a.Version, &def, &a.Enabled, &created); err != nil {
		return nil, err
	}
	a.Definition = json.RawMessage(def)
	var err error
	if a.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return nil, fmt.Errorf("parse created_at %q: %w", created, err)
	}
	return &a, nil
}
