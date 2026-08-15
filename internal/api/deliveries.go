package api

import (
	"context"

	"github.com/mcondie/sonata/internal/store"
)

func (s *Server) deliveryList(ctx context.Context, req *ListDeliveriesRequest) (*ListDeliveriesResponse, error) {
	if req.Limit < 0 {
		return nil, invalidf("limit must be >= 0")
	}
	ds, err := s.opts.Store.ListDeliveries(ctx, store.DeliveryListOptions{
		Action:    req.Action,
		State:     req.State,
		MessageID: req.MessageID,
		Limit:     req.Limit,
		BeforeID:  req.BeforeID,
	})
	if err != nil {
		return nil, err
	}
	resp := &ListDeliveriesResponse{Deliveries: make([]Delivery, 0, len(ds))}
	for _, d := range ds {
		resp.Deliveries = append(resp.Deliveries, toAPIDelivery(d))
	}
	return resp, nil
}

func (s *Server) deliveryShow(ctx context.Context, req *ShowDeliveryRequest) (*Delivery, error) {
	if req.ID == "" {
		return nil, invalidf("id is required")
	}
	d, err := s.opts.Store.GetDelivery(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	resp := toAPIDelivery(d)
	return &resp, nil
}

func (s *Server) deliveryReplay(ctx context.Context, req *ReplayDeliveryRequest) (*Delivery, error) {
	if req.ID == "" {
		return nil, invalidf("id is required")
	}
	d, err := s.opts.Store.ReplayDelivery(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if s.opts.Scheduler != nil {
		s.opts.Scheduler.WorkAdded(1)
	}
	resp := toAPIDelivery(d)
	return &resp, nil
}

func toAPIDelivery(d *store.Delivery) Delivery {
	return Delivery{
		ID:            d.ID,
		MessageID:     d.MessageID,
		ActionName:    d.ActionName,
		ActionVersion: d.ActionVersion,
		State:         d.State,
		Attempt:       d.Attempt,
		NotBefore:     d.NotBefore,
		StderrTail:    d.StderrTail,
		Error:         d.Error,
		ClaimedAt:     d.ClaimedAt,
		CompletedAt:   d.CompletedAt,
	}
}
