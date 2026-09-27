// Package codereview (fixed) is the corrected version of the concurrency
// code-review exercise in codereview/orders.go. The same test file passes
// here, with -race, and go vet is clean.
//
// Comments tagged "FIX n" point back to ISSUE n in the annotated file,
// answers/codereview/orders.go, which explains what was wrong and why.
//
// # Locking rules for this type
//
// Writing these down (in real code, too) is half the fix:
//   - mu guards orders, history, and changed.
//   - statsMu guards byStatus.
//   - If you need both, take mu first, then statsMu. Never the reverse.
//   - Never hold a lock while calling the gateway, sending on a channel, or
//     calling another method that takes the same lock.
//   - Only publish and Close touch the events channel, and they coordinate
//     through pubMu so nothing sends after it's closed.
package codereview

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	StatusCharging  Status = "charging"  // FIX 5: a charge is in flight
	StatusRefunding Status = "refunding" // FIX 6: a refund is in flight
	StatusPaid      Status = "paid"
	StatusRefunded  Status = "refunded"
	StatusCancelled Status = "cancelled"
)

// maxConcurrentCharges caps how many gateway requests ChargeAll makes at
// once. The gateway allows at most 8 per client.
const maxConcurrentCharges = 8

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

	// FIX 19, 20, 21: publish and Close coordinate through pubMu, and
	// notifyDone lets Close wait for the notifier to finish.
	events     chan Event
	pubMu      sync.RWMutex // guards closed; held (shared) while sending on events
	closed     bool
	closeOnce  sync.Once
	notifyDone chan struct{}

	mu      sync.Mutex // guards orders, history, and changed
	orders  map[string]*Order
	history []Event
	changed chan struct{} // FIX 17: closed and replaced on every status change

	statsMu  sync.Mutex // guards byStatus; take after mu, never before
	byStatus map[Status]int

	requests atomic.Int64 // FIX 9: an atomic type can't be read non-atomically by accident

	rates func() map[string]float64 // FIX 10: sync.OnceValue, loads on first use
}

// New returns a Service that uses gw and starts its background notifier.
// Call Close when you're done with it.
func New(gw Gateway) *Service {
	s := &Service{
		gateway:    gw,
		events:     make(chan Event, 16),
		notifyDone: make(chan struct{}),
		orders:     make(map[string]*Order),
		changed:    make(chan struct{}),
		byStatus:   make(map[Status]int),
		rates:      sync.OnceValue(loadRates),
	}
	go s.notify()
	return s
}

// notify records every published event until the service is closed.
func (s *Service) notify() {
	defer close(s.notifyDone)
	for ev := range s.events {
		// FIX 8: the same lock order as Stats (mu, then statsMu).
		s.mu.Lock()
		s.history = append(s.history, ev)
		s.statsMu.Lock()
		s.byStatus[ev.Status]++
		s.statsMu.Unlock()
		s.mu.Unlock()
	}
}

// publish queues ev for the notifier. Events published after Close are
// dropped. The caller must NOT hold s.mu: the send can block until the
// notifier catches up, and the notifier needs s.mu to make progress.
func (s *Service) publish(ev Event) {
	s.pubMu.RLock()
	defer s.pubMu.RUnlock()
	if s.closed {
		return
	}
	s.events <- ev
}

// setStatus changes an order's status and wakes anyone in WaitForStatus.
// The caller must hold s.mu.
func (s *Service) setStatus(o *Order, st Status) {
	o.Status = st
	close(s.changed) // closing a channel wakes every goroutine waiting on it
	s.changed = make(chan struct{})
}

// Create stores a new pending order.
func (s *Service) Create(id string, total Cents) error {
	if err := s.insert(id, total); err != nil {
		return err
	}
	s.publish(Event{OrderID: id, Status: StatusPending}) // FIX 2: after unlocking
	return nil
}

// insert does Create's locked work. Pulling it into its own small function
// means `defer s.mu.Unlock()` covers every return path (FIX 1) while the
// publish above still happens outside the lock.
func (s *Service) insert(id string, total Cents) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.orders[id]; exists {
		return fmt.Errorf("order %s already exists", id)
	}
	s.orders[id] = &Order{ID: id, Total: total, Status: StatusPending}
	return nil
}

// Get returns a copy of the order with the given ID.
//
// FIX 3: returning a copy (Order, not *Order) means callers can read and
// change it freely without racing with the service.
func (s *Service) Get(id string) (Order, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.orders[id]
	if !ok {
		return Order{}, ErrNotFound
	}
	return *o, nil
}

// Cancel cancels a pending order.
func (s *Service) Cancel(id string) error {
	// FIX 4: look the order up directly instead of calling Get, which would
	// try to take s.mu a second time.
	s.mu.Lock()
	o, ok := s.orders[id]
	if !ok {
		s.mu.Unlock()
		return ErrNotFound
	}
	if st := o.Status; st != StatusPending {
		s.mu.Unlock()
		return fmt.Errorf("order %s is %s; only pending orders can be cancelled", id, st)
	}
	s.setStatus(o, StatusCancelled)
	s.mu.Unlock()

	s.publish(Event{OrderID: id, Status: StatusCancelled})
	return nil
}

// claim atomically moves an order from one status to another, and returns
// its total. Because the check and the change happen under one lock, only
// one caller can ever claim a given order.
func (s *Service) claim(id string, from, to Status) (Cents, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	o, ok := s.orders[id]
	if !ok {
		return 0, ErrNotFound
	}
	if o.Status != from {
		return 0, fmt.Errorf("order %s is %s, not %s", id, o.Status, from)
	}
	s.setStatus(o, to)
	return o.Total, nil
}

// finish moves an order out of an in-flight status, to next.
func (s *Service) finish(id string, next Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStatus(s.orders[id], next)
}

// Charge charges a pending order through the gateway.
func (s *Service) Charge(ctx context.Context, id string) error {
	// FIX 5: claim the order (pending -> charging) in the same critical
	// section as the check. A second concurrent Charge now sees "charging"
	// and stops, instead of charging the customer again.
	amount, err := s.claim(id, StatusPending, StatusCharging)
	if err != nil {
		return err
	}

	s.requests.Add(1)
	if err := s.gateway.Charge(ctx, id, amount); err != nil {
		s.finish(id, StatusPending) // release the claim so it can be retried
		return fmt.Errorf("charge %s: %w", id, err)
	}

	s.finish(id, StatusPaid)
	s.publish(Event{OrderID: id, Status: StatusPaid})
	return nil
}

// Refund refunds a paid order through the gateway.
func (s *Service) Refund(ctx context.Context, id string) error {
	// FIX 6: the same claim pattern as Charge. No lock is held during the
	// gateway call, so one slow refund can't freeze the service.
	amount, err := s.claim(id, StatusPaid, StatusRefunding)
	if err != nil {
		return err
	}

	s.requests.Add(1)
	if err := s.gateway.Refund(ctx, id, amount); err != nil {
		s.finish(id, StatusPaid)
		return fmt.Errorf("refund %s: %w", id, err)
	}

	s.finish(id, StatusRefunded)
	s.publish(Event{OrderID: id, Status: StatusRefunded})
	return nil
}

// Count reports how many orders the service holds.
func (s *Service) Count() int { // FIX 7: pointer receiver
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// Stats returns a snapshot of the service's counters.
func (s *Service) Stats() Stats {
	// FIX 8: mu first, then statsMu, the same order as notify. With every
	// goroutine taking the locks in the same order, no cycle of waiting
	// (and so no deadlock) is possible.
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsMu.Lock()
	defer s.statsMu.Unlock()

	return Stats{Orders: len(s.orders), ByStatus: maps.Clone(s.byStatus)}
}

// Requests reports how many gateway calls the service has made.
func (s *Service) Requests() int64 {
	return s.requests.Load() // FIX 9
}

// History returns a copy of every status change recorded so far, oldest
// first.
func (s *Service) History() []Event {
	s.mu.Lock() // FIX 18
	defer s.mu.Unlock()
	return slices.Clone(s.history)
}

// loadRates stands in for fetching exchange rates from an API.
var loadRates = func() map[string]float64 {
	return map[string]float64{"USD": 1, "EUR": 0.92, "GBP": 0.79}
}

// Rate returns the exchange rate from USD to currency.
func (s *Service) Rate(currency string) (float64, bool) {
	// FIX 10: s.rates is a sync.OnceValue. The first caller runs loadRates,
	// concurrent callers wait for it, and everyone after that gets the
	// cached map. The map is never written again, so reading it
	// concurrently is safe.
	r, ok := s.rates()[currency]
	return r, ok
}

// quoteCarrier stands in for a call to a shipping carrier's API.
var quoteCarrier = func(ctx context.Context, orderID string) (Cents, error) {
	select {
	case <-time.After(200 * time.Millisecond):
		return 599, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// QuoteShipping asks the carrier for a shipping price, giving up after
// 100ms.
func (s *Service) QuoteShipping(ctx context.Context, orderID string) (Cents, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel() // FIX 12

	type result struct {
		quote Cents
		err   error
	}
	// FIX 11: a buffer of 1 lets the goroutine deliver its result and exit
	// even after we've stopped listening. Passing ctx lets the carrier call
	// itself stop early too.
	ch := make(chan result, 1)
	go func() {
		q, err := quoteCarrier(ctx, orderID)
		ch <- result{q, err}
	}()

	select {
	case r := <-ch:
		return r.quote, r.err
	case <-ctx.Done():
		return 0, fmt.Errorf("shipping quote for %s: %w", orderID, ctx.Err())
	}
}

// ChargeAll charges every pending order, at most maxConcurrentCharges at a
// time, and reports how many charges succeeded.
func (s *Service) ChargeAll(ctx context.Context) int {
	// FIX 13: snapshot the IDs under the lock, then work from the snapshot.
	s.mu.Lock()
	var ids []string
	for id, o := range s.orders {
		if o.Status == StatusPending {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()

	var (
		wg        sync.WaitGroup                              // FIX 14: one WaitGroup, never copied
		succeeded atomic.Int64                                // FIX 15
		sem       = make(chan struct{}, maxConcurrentCharges) // FIX 16
	)
	for _, id := range ids {
		sem <- struct{}{} // take a slot; blocks while all slots are in use
		wg.Go(func() {
			defer func() { <-sem }() // give the slot back
			if err := s.Charge(ctx, id); err == nil {
				succeeded.Add(1)
			}
		})
	}
	wg.Wait()
	return int(succeeded.Load())
}

// WaitForStatus blocks until the order reaches want, or ctx is done.
func (s *Service) WaitForStatus(ctx context.Context, id string, want Status) error {
	for {
		s.mu.Lock()
		o, ok := s.orders[id]
		if !ok {
			s.mu.Unlock()
			return ErrNotFound
		}
		if o.Status == want {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()

		// FIX 17: sleep until something changes (or ctx ends) instead of
		// spinning. setStatus closes `changed`, which wakes every waiter at
		// once. Then we loop and check again.
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops the background notifier, after it has recorded every event
// published before Close was called. It is safe to call more than once,
// and from multiple goroutines.
func (s *Service) Close() {
	s.closeOnce.Do(func() { // FIX 19
		// FIX 20: taking pubMu exclusively waits for any publish that's
		// mid-send, and setting closed stops new ones. Only then is it
		// safe to close the channel.
		s.pubMu.Lock()
		s.closed = true
		close(s.events)
		s.pubMu.Unlock()
	})
	<-s.notifyDone // FIX 21: wait until the notifier has drained the channel
}
