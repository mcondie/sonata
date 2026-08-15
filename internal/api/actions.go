package api

import (
	"context"
	"encoding/json"

	"github.com/mcondie/sonata/internal/store"
	"github.com/mcondie/sonata/internal/workflow"
)

// actionApply parses and validates the definition, then registers it. The
// daemon never trusts the client's validation: the CLI is one caller among
// several, and ad-hoc JSON callers post here directly.
func (s *Server) actionApply(ctx context.Context, req *ApplyActionRequest) (*ApplyActionResponse, error) {
	if len(req.Definition) == 0 || string(req.Definition) == "null" {
		return nil, invalidf("definition is required")
	}
	def, err := workflow.ParseJSON(req.Definition)
	if err != nil {
		return nil, err
	}
	// Store the canonical form, not the caller's bytes: byte equality against
	// the current version is what makes a re-apply a no-op.
	canonical, err := def.Canonical()
	if err != nil {
		return nil, err
	}

	a, changed, err := s.opts.Store.ApplyAction(ctx, def.Name, canonical)
	if err != nil {
		return nil, err
	}
	return &ApplyActionResponse{
		Name:    a.Name,
		Version: a.Version,
		Changed: changed,
		Enabled: a.Enabled,
	}, nil
}

func (s *Server) actionList(ctx context.Context, _ *ListActionsRequest) (*ListActionsResponse, error) {
	actions, err := s.opts.Store.ListActions(ctx)
	if err != nil {
		return nil, err
	}
	resp := &ListActionsResponse{Actions: make([]ActionSummary, 0, len(actions))}
	for _, a := range actions {
		sum := ActionSummary{
			Name:      a.Name,
			Version:   a.Version,
			Enabled:   a.Enabled,
			Inputs:    []string{},
			CreatedAt: a.CreatedAt,
		}
		// Stored definitions are always valid, but a listing must not fail on
		// one bad row: report what the row says and leave the rest blank.
		var def workflow.Action
		if err := json.Unmarshal(a.Definition, &def); err == nil {
			sum.Actor = def.Actor
			sum.Inputs = def.Queues()
			sum.Output = def.Output
		}
		resp.Actions = append(resp.Actions, sum)
	}
	return resp, nil
}

func (s *Server) actionShow(ctx context.Context, req *ShowActionRequest) (*Action, error) {
	if req.Name == "" {
		return nil, invalidf("name is required")
	}
	if req.Version < 0 {
		return nil, invalidf("version must be >= 0 (0 means the current version)")
	}
	a, err := s.opts.Store.GetAction(ctx, req.Name, req.Version)
	if err != nil {
		return nil, err
	}
	resp := toAPIAction(a)
	return &resp, nil
}

func (s *Server) actionEnable(ctx context.Context, req *SetActionEnabledRequest) (*SetActionEnabledResponse, error) {
	return s.setActionEnabled(ctx, req, true)
}

func (s *Server) actionDisable(ctx context.Context, req *SetActionEnabledRequest) (*SetActionEnabledResponse, error) {
	return s.setActionEnabled(ctx, req, false)
}

func (s *Server) setActionEnabled(ctx context.Context, req *SetActionEnabledRequest, enabled bool) (*SetActionEnabledResponse, error) {
	if req.Name == "" {
		return nil, invalidf("name is required")
	}
	a, err := s.opts.Store.SetActionEnabled(ctx, req.Name, enabled)
	if err != nil {
		return nil, err
	}
	// Disable cancels outstanding work — a disabled action's pending and
	// retry-waiting deliveries would otherwise sit non-terminal forever,
	// keeping the daemon busy and blocking prune. The scheduler executes the
	// transition so it cannot race a concurrent claim.
	if !enabled && s.opts.Scheduler != nil {
		if n, err := s.opts.Scheduler.CancelAction(ctx, a.Name); err != nil {
			return nil, err
		} else if n > 0 {
			s.opts.Log.Info("cancelled deliveries on disable", "action", a.Name, "count", n)
		}
	}
	return &SetActionEnabledResponse{Name: a.Name, Version: a.Version, Enabled: a.Enabled}, nil
}

func toAPIAction(a *store.Action) Action {
	return Action{
		Name:       a.Name,
		Version:    a.Version,
		Definition: a.Definition,
		Enabled:    a.Enabled,
		CreatedAt:  a.CreatedAt,
	}
}
