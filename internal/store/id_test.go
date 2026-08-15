package store

import (
	"regexp"
	"sort"
	"testing"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIDFormat(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := NewID()
		if !uuidRE.MatchString(id) {
			t.Fatalf("id %q is not a UUIDv7", id)
		}
	}
}

// The queue ordering is ORDER BY id, so IDs must be strictly increasing even
// when many are generated within one millisecond.
func TestNewIDMonotonicBurst(t *testing.T) {
	const n = 10000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = NewID()
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("burst of ids is not sorted")
	}
	for i := 1; i < n; i++ {
		if ids[i] == ids[i-1] {
			t.Fatalf("duplicate id at %d: %s", i, ids[i])
		}
	}
}

func TestNewIDMonotonicConcurrent(t *testing.T) {
	const workers, per = 8, 1000
	ch := make(chan []string, workers)
	for w := 0; w < workers; w++ {
		go func() {
			ids := make([]string, per)
			for i := range ids {
				ids[i] = NewID()
			}
			ch <- ids
		}()
	}
	seen := make(map[string]bool, workers*per)
	for w := 0; w < workers; w++ {
		for _, id := range <-ch {
			if seen[id] {
				t.Fatalf("duplicate id across goroutines: %s", id)
			}
			seen[id] = true
		}
	}
}
