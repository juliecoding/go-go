//go:build codereview

// Tests for the concurrency code-review exercise. Most of them FAIL against
// orders.go, and that's the point: each failure is a lead to debug.
//
//	go test -tags codereview -race ./codereview
//	go test -tags codereview -race -run TestClose ./codereview   # just one group
//
// Tips:
//   - "did not finish within ..." means something is stuck. Ask who holds
//     which lock, and who is waiting on which channel. A goroutine dump
//     helps: run with -timeout=10s and Go prints every goroutine's stack
//     when the timeout fires, showing exactly where each one is blocked.
//   - A "DATA RACE" report prints two stack traces: one access, and the
//     earlier conflicting access. Read both. The bug is that nothing orders
//     them.
//   - Some tests only fail under -race. Without it they may pass by luck.
//   - The same test file also runs against the fixed version in
//     answers/codereview/fixed, where every test passes.
package codereview

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// --- helpers ---

// fakeGateway is a Gateway for tests. Each hook is optional.
type fakeGateway struct {
	charge func(orderID string) error
	refund func(orderID string) error
}

func (g *fakeGateway) Charge(ctx context.Context, orderID string, amount Cents) error {
	if g.charge != nil {
		return g.charge(orderID)
	}
	return nil
}

func (g *fakeGateway) Refund(ctx context.Context, orderID string, amount Cents) error {
	if g.refund != nil {
		return g.refund(orderID)
	}
	return nil
}

// newService returns a service with a gateway that always succeeds.
func newService(t *testing.T) *Service {
	t.Helper()
	return New(&fakeGateway{})
}

// seed stores pending orders directly, skipping Create, so a bug in Create
// can't hide bugs elsewhere.
func seed(s *Service, ids ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		s.orders[id] = &Order{ID: id, Total: 1200, Status: StatusPending}
	}
}

func status(s *Service, id string) Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.orders[id].Status
}

// finishesWithin runs f and fails the test if it takes longer than d.
// A test that hangs forever is much harder to debug than one that fails.
func finishesWithin(t *testing.T, d time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not finish within %v. Is something stuck?", what, d)
	}
}

// panics reports whether f panics, and with what.
func panics(f func()) (v any) {
	defer func() { v = recover() }()
	f()
	return nil
}

// --- hangs and deadlocks ---

func TestCreateDuplicateThenAnother(t *testing.T) {
	s := newService(t)
	finishesWithin(t, time.Second, "three Creates", func() {
		if err := s.Create("A1", 1200); err != nil {
			t.Errorf("first Create: %v", err)
		}
		if err := s.Create("A1", 1200); err == nil {
			t.Error("duplicate Create should fail")
		}
		if err := s.Create("B2", 1200); err != nil {
			t.Errorf("Create after a duplicate: %v", err)
		}
	})
}

func TestCreateManyOrders(t *testing.T) {
	s := newService(t)
	finishesWithin(t, 5*time.Second, "1,000 Creates", func() {
		for i := range 1000 {
			if err := s.Create(fmt.Sprint(i), 1200); err != nil {
				t.Errorf("Create %d: %v", i, err)
				return
			}
		}
	})
}

func TestCancel(t *testing.T) {
	s := newService(t)
	seed(s, "A1")
	finishesWithin(t, time.Second, "Cancel", func() {
		if err := s.Cancel("A1"); err != nil {
			t.Errorf("Cancel: %v", err)
		}
	})
}

func TestRefundDoesNotBlockOtherCallers(t *testing.T) {
	arrived := make(chan struct{})
	release := make(chan struct{})
	s := New(&fakeGateway{refund: func(string) error {
		close(arrived)
		<-release // a slow payment gateway
		return nil
	}})
	defer close(release)
	seed(s, "A1", "B2")
	s.mu.Lock()
	s.orders["A1"].Status = StatusPaid
	s.mu.Unlock()

	go s.Refund(t.Context(), "A1")
	<-arrived // the refund is now waiting on the gateway

	// One slow refund shouldn't freeze every other order.
	finishesWithin(t, time.Second, "Get during a slow refund", func() { s.Get("B2") })
}

func TestStatsDoesNotDeadlock(t *testing.T) {
	s := newService(t)
	finishesWithin(t, 5*time.Second, "Stats while events are recorded", func() {
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := range 500 {
				s.publish(Event{OrderID: fmt.Sprint(i), Status: StatusPending})
			}
		})
		wg.Go(func() {
			for range 500 {
				s.Stats()
			}
		})
		wg.Wait()
	})
}

func TestChargeAllCountsEverySuccess(t *testing.T) {
	s := newService(t)
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprintf("order-%d", i)
	}
	seed(s, ids...)

	var n int
	finishesWithin(t, 5*time.Second, "ChargeAll", func() { n = s.ChargeAll(t.Context()) })
	if n != len(ids) {
		t.Errorf("ChargeAll reported %d successes, want %d", n, len(ids))
	}
	for _, id := range ids {
		if got := status(s, id); got != StatusPaid {
			t.Errorf("order %s status = %q after ChargeAll, want paid", id, got)
		}
	}
}

func TestChargeAllLimitsConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int64
	s := New(&fakeGateway{charge: func(string) error {
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		return nil
	}})
	ids := make([]string, 100)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	seed(s, ids...)

	finishesWithin(t, 10*time.Second, "ChargeAll", func() { s.ChargeAll(t.Context()) })
	// The gateway allows at most 8 concurrent requests per client.
	if got := peak.Load(); got > 8 {
		t.Errorf("gateway saw %d concurrent charges, want at most 8", got)
	}
}

// --- lost updates and double work ---

func TestConcurrentChargesOfOneOrderChargeOnce(t *testing.T) {
	var charges atomic.Int64
	s := New(&fakeGateway{charge: func(string) error {
		charges.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}})
	seed(s, "A1")

	// A double-clicked "Pay" button, or a client retrying.
	var wg sync.WaitGroup
	for range 5 {
		wg.Go(func() { s.Charge(t.Context(), "A1") })
	}
	finishesWithin(t, 5*time.Second, "concurrent Charges", wg.Wait)

	if n := charges.Load(); n != 1 {
		t.Errorf("the customer was charged %d times, want 1", n)
	}
}

func TestGetReturnsACopy(t *testing.T) {
	s := newService(t)
	seed(s, "A1")

	o, err := s.Get("A1")
	if err != nil {
		t.Fatal(err)
	}
	o.Status = StatusRefunded // changing what Get returned...

	if got := status(s, "A1"); got != StatusPending { // ...must not change the stored order
		t.Errorf("changing Get's result changed the stored order to %q", got)
	}
}

// This test passes against the exercise code: WaitForStatus does return
// once the order is paid. It still has a real problem, though, and no test
// here will show it to you. Read it closely.
func TestWaitForStatus(t *testing.T) {
	s := newService(t)
	seed(s, "A1")
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.Charge(context.Background(), "A1")
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := s.WaitForStatus(ctx, "A1", StatusPaid); err != nil {
		t.Errorf("WaitForStatus: %v", err)
	}
}

// --- Close ---

func TestCloseTwice(t *testing.T) {
	s := newService(t)
	s.Close()
	if v := panics(s.Close); v != nil {
		t.Errorf("second Close panicked: %v", v)
	}
}

func TestChargeAfterClose(t *testing.T) {
	s := newService(t)
	seed(s, "A1")
	s.Close()

	// A request that was already in flight when the service shut down.
	if v := panics(func() { s.Charge(t.Context(), "A1") }); v != nil {
		t.Errorf("Charge after Close panicked: %v", v)
	}
}

func TestCloseRecordsPendingEvents(t *testing.T) {
	s := newService(t)
	for i := range 10 {
		s.publish(Event{OrderID: fmt.Sprint(i), Status: StatusPending})
	}
	s.Close()

	// Everything published before Close should be in the history once
	// Close returns.
	s.mu.Lock()
	n := len(s.history)
	s.mu.Unlock()
	if n != 10 {
		t.Errorf("history has %d events after Close, want 10", n)
	}
}

// --- data races (run with -race) ---

func TestCountIsRaceFree(t *testing.T) {
	s := newService(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 100 {
			seed(s, fmt.Sprint(i)) // seed writes while holding the lock
		}
	})
	wg.Go(func() {
		for range 100 {
			s.Count()
		}
	})
	wg.Wait()
}

func TestRequestsIsRaceFree(t *testing.T) {
	s := newService(t)
	ids := make([]string, 50)
	for i := range ids {
		ids[i] = fmt.Sprint(i)
	}
	seed(s, ids...)

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() { s.Charge(t.Context(), id) })
	}
	wg.Go(func() {
		for range 100 {
			s.Requests()
		}
	})
	wg.Wait()
}

func TestHistoryIsRaceFree(t *testing.T) {
	s := newService(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 100 {
			s.publish(Event{OrderID: fmt.Sprint(i), Status: StatusPending})
		}
	})
	wg.Go(func() {
		for range 100 {
			for _, ev := range s.History() {
				_ = ev.OrderID
			}
		}
	})
	wg.Wait()
}

func TestRateLoadsOnce(t *testing.T) {
	var loads atomic.Int64
	orig := loadRates
	loadRates = func() map[string]float64 {
		loads.Add(1)
		time.Sleep(time.Millisecond) // a real API call takes a while
		return orig()
	}
	t.Cleanup(func() { loadRates = orig })

	s := newService(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if r, ok := s.Rate("EUR"); !ok || r != 0.92 {
				t.Errorf("Rate(EUR) = %v, %v", r, ok)
			}
		})
	}
	wg.Wait()
	if n := loads.Load(); n != 1 {
		t.Errorf("rates were loaded %d times, want 1", n)
	}
}

// --- goroutine leaks ---

// This test is last on purpose. When synctest finds a goroutine that's
// stuck forever, it panics ("deadlock: main bubble goroutine has exited but
// blocked goroutines remain"), and a panic ends the whole test run. The
// stack trace printed under the panic shows where the leaked goroutine is
// stuck.
func TestQuoteShippingDoesNotLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newService(t)
		defer s.Close()

		// The carrier takes 200ms, and QuoteShipping gives up at 100ms.
		if _, err := s.QuoteShipping(t.Context(), "A1"); err == nil {
			t.Fatal("expected a timeout error")
		}
		time.Sleep(time.Second) // plenty of time for the carrier call to finish
	})
}
