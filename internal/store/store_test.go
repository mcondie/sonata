package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testMessage(queue string) *Message {
	return &Message{
		ID:        NewID(),
		Queue:     queue,
		Payload:   json.RawMessage(`{"n":1}`),
		Headers:   json.RawMessage(`{"hops":0}`),
		TraceID:   NewID(),
		CreatedAt: time.Now().UTC(),
	}
}

func TestMigrateIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	ctx := context.Background()

	s1, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := s1.AppendMessage(ctx, testMessage("q")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Second open must apply nothing and lose nothing.
	s2, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer s2.Close()
	msgs, err := s2.ListMessages(ctx, ListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages after reopen, want 1", len(msgs))
	}
}

func TestAppendGetRoundTrip(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	origin := "builder"
	var version int64 = 3
	parent := testMessage("parents")
	if err := s.AppendMessage(ctx, parent); err != nil {
		t.Fatalf("append parent: %v", err)
	}

	m := testMessage("child.queue")
	m.OriginAction = &origin
	m.OriginActionVersion = &version
	m.OriginMessageID = &parent.ID
	if err := s.AppendMessage(ctx, m); err != nil {
		t.Fatalf("append: %v", err)
	}

	got, err := s.GetMessage(ctx, m.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Queue != m.Queue || got.TraceID != m.TraceID {
		t.Errorf("got %+v, want %+v", got, m)
	}
	if string(got.Payload) != `{"n":1}` || string(got.Headers) != `{"hops":0}` {
		t.Errorf("payload/headers mangled: %s %s", got.Payload, got.Headers)
	}
	if got.OriginAction == nil || *got.OriginAction != origin {
		t.Errorf("origin_action = %v, want %q", got.OriginAction, origin)
	}
	if got.OriginMessageID == nil || *got.OriginMessageID != parent.ID {
		t.Errorf("origin_message_id = %v, want %q", got.OriginMessageID, parent.ID)
	}
	if !got.CreatedAt.Equal(m.CreatedAt) {
		t.Errorf("created_at = %v, want %v", got.CreatedAt, m.CreatedAt)
	}
}

func TestGetMessageNotFound(t *testing.T) {
	s := openTest(t)
	if _, err := s.GetMessage(context.Background(), "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestListFiltersAndPagination(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	var aIDs []string
	for i := 0; i < 5; i++ {
		m := testMessage("a")
		if err := s.AppendMessage(ctx, m); err != nil {
			t.Fatalf("append: %v", err)
		}
		aIDs = append(aIDs, m.ID)
	}
	traced := testMessage("b")
	if err := s.AppendMessage(ctx, traced); err != nil {
		t.Fatalf("append: %v", err)
	}

	// Queue filter, newest first.
	msgs, err := s.ListMessages(ctx, ListOptions{Queue: "a"})
	if err != nil {
		t.Fatalf("list a: %v", err)
	}
	if len(msgs) != 5 || msgs[0].ID != aIDs[4] || msgs[4].ID != aIDs[0] {
		t.Fatalf("queue filter/order wrong: %d messages, first %s", len(msgs), msgs[0].ID)
	}

	// Trace filter.
	msgs, err = s.ListMessages(ctx, ListOptions{TraceID: traced.TraceID})
	if err != nil {
		t.Fatalf("list trace: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != traced.ID {
		t.Fatalf("trace filter wrong: %+v", msgs)
	}

	// Keyset pagination: two pages of 2 walk backwards without overlap.
	page1, err := s.ListMessages(ctx, ListOptions{Queue: "a", Limit: 2})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	page2, err := s.ListMessages(ctx, ListOptions{Queue: "a", Limit: 2, BeforeID: page1[1].ID})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if page1[0].ID != aIDs[4] || page1[1].ID != aIDs[3] ||
		page2[0].ID != aIDs[2] || page2[1].ID != aIDs[1] {
		t.Fatalf("pagination wrong: page1 %s,%s page2 %s,%s",
			page1[0].ID, page1[1].ID, page2[0].ID, page2[1].ID)
	}
}

func TestListQueues(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	for _, q := range []string{"b", "a", "b", "b"} {
		if err := s.AppendMessage(ctx, testMessage(q)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	queues, err := s.ListQueues(ctx)
	if err != nil {
		t.Fatalf("list queues: %v", err)
	}
	want := []QueueInfo{{Name: "a", Messages: 1}, {Name: "b", Messages: 3}}
	if len(queues) != 2 || queues[0] != want[0] || queues[1] != want[1] {
		t.Fatalf("queues = %+v, want %+v", queues, want)
	}
}

// Concurrent appends and reads must not surface SQLITE_BUSY: the single
// writer connection serializes writes, WAL lets readers proceed.
func TestConcurrentAppendAndList(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	const writers, per = 8, 50
	var wg sync.WaitGroup
	errs := make(chan error, writers*2)

	for w := 0; w < writers; w++ {
		wg.Add(2)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if err := s.AppendMessage(ctx, testMessage(fmt.Sprintf("q%d", w))); err != nil {
					errs <- fmt.Errorf("writer %d: %w", w, err)
					return
				}
			}
		}(w)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := s.ListMessages(ctx, ListOptions{}); err != nil {
					errs <- fmt.Errorf("reader: %w", err)
					return
				}
				if _, err := s.ListQueues(ctx); err != nil {
					errs <- fmt.Errorf("reader queues: %w", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	msgs, err := s.ListMessages(ctx, ListOptions{Limit: 1000})
	if err != nil {
		t.Fatalf("final list: %v", err)
	}
	if len(msgs) != writers*per {
		t.Fatalf("got %d messages, want %d", len(msgs), writers*per)
	}
}
