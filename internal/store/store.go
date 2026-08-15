// Package store is the daemon's SQLite layer. It is opened by the daemon
// only — the CLI talks to it through the API, never directly.
package store

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned when a queried row does not exist. The API layer
// maps it to 404.
var ErrNotFound = errors.New("not found")

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Store wraps the database with the two-handle discipline: all writes go
// through a single-connection handle issuing BEGIN IMMEDIATE, reads use a
// pooled handle. Never widen the writer pool — a multi-connection writer
// produces SQLITE_BUSY under concurrent CLI load.
type Store struct {
	w *sql.DB // writer: MaxOpenConns(1), _txlock=immediate
	r *sql.DB // readers: pooled
}

// Open opens (creating if needed) the database at path and applies pending
// migrations.
func Open(ctx context.Context, path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create database dir: %w", err)
	}

	w, err := open(path, true)
	if err != nil {
		return nil, err
	}
	r, err := open(path, false)
	if err != nil {
		_ = w.Close()
		return nil, err
	}
	s := &Store{w: w, r: r}

	if err := s.migrate(ctx); err != nil {
		_ = s.Close()
		return nil, err
	}
	return s, nil
}

func open(path string, writer bool) (*sql.DB, error) {
	q := url.Values{}
	// _txlock makes every transaction on this handle BEGIN IMMEDIATE, which
	// takes the write lock up front. A deferred transaction upgrading from a
	// read lock mid-transaction deadlocks in a way busy_timeout cannot
	// resolve.
	if writer {
		q.Set("_txlock", "immediate")
	}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")

	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if writer {
		db.SetMaxOpenConns(1)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open database %s: %w", path, err)
	}
	return db, nil
}

// Close closes both handles.
func (s *Store) Close() error {
	rErr := s.r.Close()
	wErr := s.w.Close()
	if wErr != nil {
		return wErr
	}
	return rErr
}

// write runs fn inside a write transaction. This is the only way anything in
// the daemon mutates the database.
func (s *Store) write(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin write: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// migrate applies embedded migrations in filename order, each in its own
// transaction, tracked in schema_migrations.
func (s *Store) migrate(ctx context.Context) error {
	if _, err := s.w.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var applied int
		err := s.r.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schema_migrations WHERE name = ?`, name).Scan(&applied)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied > 0 {
			continue
		}

		src, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		err = s.write(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, string(src)); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx,
				`INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`,
				name, nowUTC())
			return err
		})
		if err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
	}
	return nil
}
