//go:build codereview

// Package codereview is a concurrency code-review and debugging exercise for
// two people.
//
// It's a small order service: create orders, charge and refund them through
// a payment gateway, and keep a history of status changes, recorded by a
// background goroutine. It compiles, and at a glance it looks like
// reasonable code.
//
// Every planted problem is a concurrency problem: goroutines, channels,
// mutexes, atomics, WaitGroups, or contexts. Everything else (naming, error
// handling, money as integer cents, doc comments) is deliberately fine, so
// if an issue isn't about concurrency, it isn't one of the planted bugs.
// There are about 20, plus one thing that looks like a bug but isn't.
//
// # How to use it
//
//  1. Review (about 30 minutes). One of you plays the author and the other
//     the reviewer; swap halfway if you like. Read the code cold and leave
//     comments the way you would on a real pull request. For each problem,
//     say what's wrong, what could happen in production, and how to fix it.
//     For every mutex, ask: what does it guard, and is every access to that
//     data under it? For every goroutine, ask: how does it stop?
//
//  2. Debug. Run the tests and chase down each failure:
//
//     go test -tags codereview -race ./codereview
//
//     Then try go vet, which knows several concurrency mistakes by name:
//
//     go vet -tags codereview ./codereview
//
//     Some problems show up as hangs, some only under -race, some only in
//     go vet, and a couple only by reading.
//
//  3. Compare notes with answers/codereview/orders.go (the same code,
//     annotated issue by issue) and answers/codereview/fixed/ (a corrected
//     version that passes every test).
//
// The //go:build codereview line at the top keeps this deliberately broken
// package out of normal builds, so `go build ./...` and `go vet ./...` stay
// clean for the rest of the repo.
package codereview

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

// Cents is an amount of money in the smallest currency unit.
type Cents int64

// Status is where an order is in its lifecycle.
type Status string

const (
	StatusPending   Status = "pending"
	StatusPaid      Status = "paid"
	StatusRefunded  Status = "refunded"
	StatusCancelled Status = "cancelled"
)

// Order is a customer's order.
type Order struct {
	ID     string
	Total  Cents
	Status Status
}

// Event records an order changing status.
type Event struct {
	OrderID string
	Status  Status
}

// Stats is a snapshot of the service's counters.
type Stats struct {
	Orders   int
	ByStatus map[Status]int
}

// Gateway is the payment provider.
type Gateway interface {
	Charge(ctx context.Context, orderID string, amount Cents) error
	Refund(ctx context.Context, orderID string, amount Cents) error
}

// ErrNotFound is returned when no order has the requested ID.
var ErrNotFound = errors.New("order not found")

// Service stores orders, charges them through a Gateway, and keeps a
// history of status changes.
type Service struct {
	gateway Gateway
	events  chan Event

	mu      sync.Mutex // guards orders and history
	orders  map[string]*Order
	history []Event

	statsMu  sync.Mutex // guards byStatus
	byStatus map[Status]int

	requests int64 // gateway calls made; updated with sync/atomic

	rates map[string]float64 // exchange rates, loaded on first use
}

// New returns a Service that uses gw and starts its background notifier.
// Call Close when you're done with it.
func New(gw Gateway) *Service {
	s := &Service{
		gateway:  gw,
		events:   make(chan Event, 16),
		orders:   make(map[string]*Order),
		byStatus: make(map[Status]int),
	}
	go s.notify()
	return s
}

// notify records every published event until the service is closed.
func (s *Service) notify() {
	for ev := range s.events {
		s.mu.Lock()
		s.history = append(s.history, ev)
		s.statsMu.Lock()
		s.byStatus[ev.Status]++
		s.statsMu.Unlock()
		s.mu.Unlock()
	}
}

func (s *Service) publish(ev Event) {
	s.events <- ev
}

// Create stores a new pending order.
func (s *Service) Create(id string, total Cents) error {
	s.mu.Lock()
	if _, exists := s.orders[id]; exists {
		return fmt.Errorf("order %s already exists", id)
	}
	s.orders[id] = &Order{ID: id, Total: total, Status: StatusPending}
	s.publish(Event{OrderID: id, Status: StatusPending})
	s.mu.Unlock()
	return nil
}

// Get returns the order with the given ID.
func (s *Service) Get(id string) (*Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.orders[id]
	if !ok {
		return nil, ErrNotFound
	}
	return o, nil
}

// Cancel cancels a pending order.
func (s *Service) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, err := s.Get(id)
	if err != nil {
		return err
	}
	if o.Status != StatusPending {
		return fmt.Errorf("order %s is %s; only pending orders can be cancelled", id, o.Status)
	}
	o.Status = StatusCancelled
	s.publish(Event{OrderID: id, Status: StatusCancelled})
	return nil
}

// Charge charges a pending order through the gateway.
func (s *Service) Charge(ctx context.Context, id string) error {
	s.mu.Lock()
	o, ok := s.orders[id]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	status, amount := o.Status, o.Total
	s.mu.Unlock()

	if status != StatusPending {
		return fmt.Errorf("order %s is %s, not pending", id, status)
	}

	atomic.AddInt64(&s.requests, 1)
	if err := s.gateway.Charge(ctx, id, amount); err != nil {
		return fmt.Errorf("charge %s: %w", id, err)
	}

	s.mu.Lock()
	o.Status = StatusPaid
	s.mu.Unlock()
	s.publish(Event{OrderID: id, Status: StatusPaid})
	return nil
}

// Refund refunds a paid order through the gateway.
func (s *Service) Refund(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.orders[id]
	if !ok {
		return ErrNotFound
	}
	if o.Status != StatusPaid {
		return fmt.Errorf("order %s is %s, not paid", id, o.Status)
	}

	atomic.AddInt64(&s.requests, 1)
	if err := s.gateway.Refund(ctx, id, o.Total); err != nil {
		return fmt.Errorf("refund %s: %w", id, err)
	}
	o.Status = StatusRefunded
	s.publish(Event{OrderID: id, Status: StatusRefunded})
	return nil
}

// Count reports how many orders the service holds.
func (s Service) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// Stats returns a snapshot of the service's counters.
func (s *Service) Stats() Stats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{Orders: len(s.orders), ByStatus: maps.Clone(s.byStatus)}
}

// Requests reports how many gateway calls the service has made.
func (s *Service) Requests() int64 {
	return s.requests
}

// History returns every status change recorded so far, oldest first.
func (s *Service) History() []Event {
	return s.history
}

// loadRates stands in for fetching exchange rates from an API.
var loadRates = func() map[string]float64 {
	return map[string]float64{"USD": 1, "EUR": 0.92, "GBP": 0.79}
}

// Rate returns the exchange rate from USD to currency.
func (s *Service) Rate(currency string) (float64, bool) {
	if s.rates == nil {
		s.rates = loadRates()
	}
	r, ok := s.rates[currency]
	return r, ok
}

// quoteCarrier stands in for a slow call to a shipping carrier's API.
var quoteCarrier = func(orderID string) Cents {
	time.Sleep(200 * time.Millisecond)
	return 599
}

// QuoteShipping asks the carrier for a shipping price, giving up after
// 100ms.
func (s *Service) QuoteShipping(ctx context.Context, orderID string) (Cents, error) {
	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)

	quote := make(chan Cents)
	go func() {
		quote <- quoteCarrier(orderID)
	}()

	select {
	case q := <-quote:
		return q, nil
	case <-ctx.Done():
		return 0, fmt.Errorf("shipping quote for %s: %w", orderID, ctx.Err())
	}
}

// ChargeAll charges every pending order concurrently and reports how many
// charges succeeded.
func (s *Service) ChargeAll(ctx context.Context) int {
	var wg sync.WaitGroup
	var succeeded int

	for id, o := range s.orders {
		if o.Status != StatusPending {
			continue
		}
		wg.Add(1)
		go func() {
			s.chargeOne(ctx, id, wg, &succeeded)
		}()
	}

	wg.Wait()
	return succeeded
}

func (s *Service) chargeOne(ctx context.Context, id string, wg sync.WaitGroup, succeeded *int) {
	defer wg.Done()
	if err := s.Charge(ctx, id); err == nil {
		*succeeded++
	}
}

// WaitForStatus blocks until the order reaches want, or ctx is done.
func (s *Service) WaitForStatus(ctx context.Context, id string, want Status) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		o, err := s.Get(id)
		if err != nil {
			return err
		}
		if o.Status == want {
			return nil
		}
	}
}

// Close stops the background notifier.
func (s *Service) Close() {
	close(s.events)
}
