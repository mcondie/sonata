package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func def(command string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"name":"a","inputs":[{"queue":"q"}],"actor":"subprocess","instructions":{"command":[%q]}}`,
		command))
}

func TestApplyActionVersions(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	first, changed, err := s.ApplyAction(ctx, "a", def("./one.sh"))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !changed || first.Version != 1 || !first.Enabled {
		t.Fatalf("first apply: %+v changed=%t", first, changed)
	}

	// Re-applying the same bytes must not burn a version — applying a
	// directory of files repeatedly is the normal case.
	same, changed, err := s.ApplyAction(ctx, "a", def("./one.sh"))
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if changed || same.Version != 1 {
		t.Fatalf("re-apply: %+v changed=%t", same, changed)
	}
	if !same.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("no-op apply reported a new created_at: %s vs %s", same.CreatedAt, first.CreatedAt)
	}

	second, changed, err := s.ApplyAction(ctx, "a", def("./two.sh"))
	if err != nil {
		t.Fatalf("edited apply: %v", err)
	}
	if !changed || second.Version != 2 {
		t.Fatalf("edited apply: %+v changed=%t", second, changed)
	}

	// Both versions stay retrievable: "which instructions ran" must remain
	// answerable after an edit.
	v1, err := s.GetAction(ctx, "a", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if string(v1.Definition) != string(def("./one.sh")) {
		t.Fatalf("v1 definition = %s", v1.Definition)
	}
	latest, err := s.GetAction(ctx, "a", LatestVersion)
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if latest.Version != 2 {
		t.Fatalf("latest version = %d", latest.Version)
	}
}

func TestGetActionNotFound(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	if _, err := s.GetAction(ctx, "missing", LatestVersion); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing action: %v", err)
	}
	if _, _, err := s.ApplyAction(ctx, "a", def("./one.sh")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := s.GetAction(ctx, "a", 7); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing version: %v", err)
	}
	if _, err := s.SetActionEnabled(ctx, "missing", false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("enable missing action: %v", err)
	}
	if _, err := s.ListActionVersions(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("versions of missing action: %v", err)
	}
}

// A new version inherits the current flag: editing a disabled action must
// never silently re-enable it.
func TestApplyCarriesEnabledForward(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	if _, _, err := s.ApplyAction(ctx, "a", def("./one.sh")); err != nil {
		t.Fatalf("apply: %v", err)
	}
	disabled, err := s.SetActionEnabled(ctx, "a", false)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if disabled.Enabled || disabled.Version != 1 {
		t.Fatalf("disable: %+v", disabled)
	}

	next, changed, err := s.ApplyAction(ctx, "a", def("./two.sh"))
	if err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	if !changed || next.Version != 2 {
		t.Fatalf("apply v2: %+v changed=%t", next, changed)
	}
	if next.Enabled {
		t.Fatal("applying an edit re-enabled a disabled action")
	}

	// Only an explicit enable brings it back.
	enabled, err := s.SetActionEnabled(ctx, "a", true)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !enabled.Enabled || enabled.Version != 2 {
		t.Fatalf("enable: %+v", enabled)
	}
	// The flag lives on the current version; older rows are untouched history.
	v1, err := s.GetAction(ctx, "a", 1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if v1.Enabled {
		t.Fatal("enable leaked onto a superseded version")
	}
}

func TestListActionsReturnsCurrentVersions(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	for _, name := range []string{"beta", "alpha"} {
		if _, _, err := s.ApplyAction(ctx, name, def("./one.sh")); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if _, _, err := s.ApplyAction(ctx, "alpha", def("./two.sh")); err != nil {
		t.Fatalf("apply alpha v2: %v", err)
	}
	if _, err := s.SetActionEnabled(ctx, "beta", false); err != nil {
		t.Fatalf("disable beta: %v", err)
	}

	list, err := s.ListActions(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("listed %d actions, want 2", len(list))
	}
	if list[0].Name != "alpha" || list[0].Version != 2 || !list[0].Enabled {
		t.Fatalf("alpha row = %+v", list[0])
	}
	if list[1].Name != "beta" || list[1].Version != 1 || list[1].Enabled {
		t.Fatalf("beta row = %+v", list[1])
	}

	versions, err := s.ListActionVersions(ctx, "alpha")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(versions) != 2 || versions[0].Version != 2 || versions[1].Version != 1 {
		t.Fatalf("versions = %+v", versions)
	}
}

// Concurrent applies of one name are the case the write() serializer exists
// for: versions must come out dense and distinct, never colliding on the
// primary key or skipping a number.
func TestApplyActionConcurrent(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	const n = 16
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		versions = map[int64]int{}
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, changed, err := s.ApplyAction(ctx, "a", def(fmt.Sprintf("./%d.sh", i)))
			if err != nil {
				t.Errorf("apply %d: %v", i, err)
				return
			}
			if !changed {
				t.Errorf("apply %d reported no change for a distinct definition", i)
			}
			mu.Lock()
			versions[a.Version]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(versions) != n {
		t.Fatalf("got %d distinct versions, want %d: %v", len(versions), n, versions)
	}
	for v := int64(1); v <= n; v++ {
		if versions[v] != 1 {
			t.Fatalf("version %d issued %d times", v, versions[v])
		}
	}

	all, err := s.ListActionVersions(ctx, "a")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	if len(all) != n {
		t.Fatalf("stored %d versions, want %d", len(all), n)
	}
}
