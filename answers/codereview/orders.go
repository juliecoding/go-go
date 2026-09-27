//go:build codereview

// Package codereview (annotated) is the answer key for the concurrency
// code-review exercise. The code below is identical to codereview/orders.go;
// the only additions are ISSUE comments explaining what's wrong, why it
// matters, and how to fix it. The corrected version is in fixed/orders.go,
// where comments tagged "FIX n" match the ISSUE numbers here.
//
// # Scorecard
//
// Severity levels:
//   - BLOCKER: hangs, crashes, double-charges, or corrupts data. Don't merge.
//   - MAJOR: a real bug that will bite under load, or eventually.
//   - MINOR: wasteful or fragile, but not wrong today.
//
// "Found by" says what can detect each issue. "reading" means no tool will
// catch it for you.
//
//	 #  Severity   Issue                                         Found by
//	--  ---------  --------------------------------------------  ------------------------------------
//	 1  BLOCKER    early return without Unlock                   TestCreateDuplicateThenAnother (hang)
//	 2  BLOCKER    channel send while holding the lock           TestCreateManyOrders (hang)
//	               the consumer needs
//	 3  BLOCKER    Get hands out a pointer to shared state       TestGetReturnsACopy
//	 4  BLOCKER    re-locking a mutex you already hold           TestCancel (hang)
//	 5  BLOCKER    check-then-act race: double charges           TestConcurrentChargesOfOneOrderChargeOnce
//	 6  MAJOR      lock held during a slow network call          TestRefundDoesNotBlockOtherCallers
//	 7  BLOCKER    value receiver copies the mutex               go vet, TestCountIsRaceFree (-race)
//	 8  BLOCKER    lock-order inversion (A→B vs B→A)             TestStatsDoesNotDeadlock (hang)
//	 9  MAJOR      atomic write, plain read                      TestRequestsIsRaceFree (-race)
//	10  MAJOR      lazy initialization without sync.Once         TestRateLoadsOnce (-race)
//	11  MAJOR      goroutine leak on timeout                     TestQuoteShippingDoesNotLeak
//	12  MINOR      context cancel func discarded                 go vet
//	13  BLOCKER    ranging over the map without the lock         reading, -race under load
//	14  BLOCKER    WaitGroup passed by value                     go vet, TestChargeAllCountsEverySuccess (hang)
//	15  BLOCKER    counter incremented from many goroutines      -race (once 14 is fixed)
//	16  MAJOR      unbounded goroutines                          TestChargeAllLimitsConcurrency
//	17  MAJOR      busy-wait loop                                reading
//	18  MAJOR      History returns shared slice, no lock         TestHistoryIsRaceFree (-race)
//	19  BLOCKER    Close twice panics                            TestCloseTwice
//	20  BLOCKER    closing a channel others still send on        TestChargeAfterClose
//	21  MAJOR      Close doesn't wait for the notifier           TestCloseRecordsPendingEvents
//	22  NOT A BUG  goroutine captures the loop variable          (fine since Go 1.22)
//
// # Questions that find most of these
//
// Every one of these bugs is an answer to one of four questions. They make
// a good checklist for any concurrent code review:
//
//  1. For each piece of shared data: which lock guards it, and is EVERY
//     read and write done while holding that lock? (Issues 3, 7, 9, 10,
//     13, 15, 18.)
//  2. For each lock: can anything slow or blocking happen while it's held
//     (network calls, channel sends, taking another lock)? And is it
//     released on every path? (1, 2, 4, 6, 8.)
//  3. For each "check, then act": can the world change between the check
//     and the act? (5.)
//  4. For each goroutine and channel: who stops it, and who closes it? Can
//     anything send after the close? (11, 14, 16, 19, 20, 21.)
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

	// (See ISSUE 9.) A plain int64 that the code promises to only touch
	// through sync/atomic. That promise is easy to break, and the compiler
	// can't check it. atomic.Int64 (Go 1.19+) makes non-atomic access
	// impossible.
	requests int64 // gateway calls made; updated with sync/atomic

	// (See ISSUE 10.) Loaded lazily, with no synchronization.
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
		// ISSUE 8 [BLOCKER] Lock-order inversion. This takes mu, THEN
		// statsMu. Stats (below) takes statsMu, THEN mu. If notify holds
		// mu and wants statsMu at the same moment Stats holds statsMu and
		// wants mu, each waits for the other forever. That's the classic
		// deadlock, sometimes called "deadly embrace". It only happens when
		// the timing lines up, so it can pass every test on a laptop and
		// then hang in production.
		// Fix: pick one global order for the locks (document it next to
		// the struct) and always follow it. Or restructure so no one ever
		// holds both at once.
		s.mu.Lock()
		s.history = append(s.history, ev)
		s.statsMu.Lock()
		s.byStatus[ev.Status]++
		s.statsMu.Unlock()
		s.mu.Unlock()
	}
}

func (s *Service) publish(ev Event) {
	// (See ISSUES 19 and 20.) This panics with "send on closed channel" if
	// Close has run, and nothing prevents that.
	s.events <- ev
}

// Create stores a new pending order.
func (s *Service) Create(id string, total Cents) error {
	s.mu.Lock()
	if _, exists := s.orders[id]; exists {
		// ISSUE 1 [BLOCKER] Returns while still holding the lock. Nobody
		// ever unlocks it, so the next call to anything that locks s.mu
		// blocks forever. One duplicate order ID freezes the whole service.
		// Fix: `defer s.mu.Unlock()` right after Lock, so every return path
		// unlocks. (If you need to do work after unlocking, like the
		// publish below, move the locked part into a small helper function
		// that uses defer.)
		return fmt.Errorf("order %s already exists", id)
	}
	s.orders[id] = &Order{ID: id, Total: total, Status: StatusPending}
	// ISSUE 2 [BLOCKER] A channel send while holding s.mu, and the goroutine
	// on the other end of the channel (notify) needs s.mu to make progress.
	// While the buffer has room this works, so small tests pass. But once
	// Create outpaces the notifier and the 16-slot buffer fills, the send
	// blocks. Create is waiting for notify to receive, while holding the
	// lock notify is waiting for. Deadlock. Cancel and Refund below make
	// the same mistake.
	// Fix: never do anything that can block (channel sends, network calls,
	// taking another lock) while holding a lock, unless you're certain
	// nothing on the other side needs that lock. Here: unlock first, then
	// publish.
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
	// ISSUE 3 [BLOCKER] The lookup is locked correctly, but it returns a
	// pointer to the service's own Order. The lock only protects the
	// lookup: as soon as Get returns, the caller can read or write
	// o.Status with no lock held, while Charge or Cancel write it under
	// the lock. That's a data race, and it lets any caller silently change
	// stored state. (WaitForStatus reads o.Status this way.) A lock
	// protects data only while all access goes through it. Handing out
	// pointers lets the data "escape" the lock.
	// Fix: return a copy (Order, not *Order). For types with slices or
	// maps inside, clone those too.
	return o, nil
}

// Cancel cancels a pending order.
func (s *Service) Cancel(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// ISSUE 4 [BLOCKER] Cancel holds s.mu, then calls Get, which tries to
	// lock s.mu again. Go's sync.Mutex is not reentrant: it doesn't know or
	// care that the same goroutine already holds it. So Get waits for
	// Cancel to unlock, which it never will, because it's waiting for Get.
	// Every call to Cancel hangs forever (and so does everything else that
	// needs s.mu). Other languages have reentrant locks; Go leaves them out
	// on purpose, and code that "needs" one is usually confused about what
	// its lock protects.
	// Fix: inside a locked section, use the data directly (s.orders[id]),
	// or split out unexported helpers documented "caller must hold s.mu".
	o, err := s.Get(id)
	if err != nil {
		return err
	}
	if o.Status != StatusPending {
		return fmt.Errorf("order %s is %s; only pending orders can be cancelled", id, o.Status)
	}
	o.Status = StatusCancelled
	s.publish(Event{OrderID: id, Status: StatusCancelled}) // ISSUE 2 again: a send while holding s.mu
	return nil
}

// Charge charges a pending order through the gateway.
func (s *Service) Charge(ctx context.Context, id string) error {
	// ISSUE 5 [BLOCKER] Check-then-act, also known as TOCTOU (time of check
	// to time of use). The status check happens under the lock, but the
	// lock is released before acting on it. Five concurrent Charges of the
	// same order (a double-clicked Pay button, a client retry) can ALL see
	// "pending", ALL unlock, and ALL call the gateway. The customer gets
	// charged five times. Each step here is individually "thread-safe",
	// but the sequence isn't. Locks make single operations atomic, not the
	// gaps between them.
	// Fix: make the check and a claim one atomic step. Under the lock,
	// check for pending AND set the status to "charging", then unlock and
	// call the gateway. A second caller sees "charging" and stops. Set
	// "paid" on success, or back to "pending" on failure.
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
	// ISSUE 6 [MAJOR] The deferred Unlock means s.mu is held for the whole
	// function, including the gateway call, which can take seconds. For
	// that entire time every Get, Create, Charge, and Stats on EVERY order
	// is stuck waiting. One slow refund freezes the service, and under
	// load, requests pile up behind the lock until timeouts cascade. It
	// "works" in tests with an instant fake gateway, which is why it
	// slips through review. (It also publishes while holding the lock:
	// ISSUE 2 again.)
	// Fix: the same claim pattern as Charge. Lock, check "paid" and set
	// "refunding", unlock, call the gateway, then lock again to record the
	// result.
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
//
// ISSUE 7 [BLOCKER] Value receiver on a type that contains a mutex. Each
// call copies the entire Service, including both mutexes, and then locks
// the COPY. Writers lock the real s.mu, and Count locks its own private
// copy, so nothing synchronizes them: a data race on the map. Worse, if the
// real mutex happens to be locked at the moment of the copy, the copy
// starts out locked, and Count blocks forever. go vet reports "passes lock
// by value".
// Fix: pointer receiver. Rule of thumb: once a type has a mutex (or a
// WaitGroup, or anything from sync), every method gets a pointer receiver
// and the value is never copied.
func (s Service) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.orders)
}

// Stats returns a snapshot of the service's counters.
func (s *Service) Stats() Stats {
	// ISSUE 8 (the other half). statsMu, THEN mu: the opposite order from
	// notify. See ISSUE 8 in notify.
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{Orders: len(s.orders), ByStatus: maps.Clone(s.byStatus)}
}

// Requests reports how many gateway calls the service has made.
func (s *Service) Requests() int64 {
	// ISSUE 9 [MAJOR] requests is written with atomic.AddInt64 but read
	// here with a plain load. Atomics only work when EVERY access is
	// atomic. One plain read is a data race, and the value can be stale
	// or torn on some platforms. -race reports it.
	// Fix: atomic.LoadInt64(&s.requests), or better, make the field an
	// atomic.Int64, whose methods are the only way to touch it.
	return s.requests
}

// History returns every status change recorded so far, oldest first.
func (s *Service) History() []Event {
	// ISSUE 18 [MAJOR] Two problems in one line. (a) It reads s.history
	// without s.mu while notify appends to it: a data race on the slice
	// header (pointer, length, capacity), so a caller can see a length
	// that doesn't match the array. (b) Even with a lock, returning the
	// slice itself shares the backing array: the caller can modify the
	// service's history, or see it change under them after the next
	// append.
	// Fix: lock, and return slices.Clone(s.history).
	return s.history
}

// loadRates stands in for fetching exchange rates from an API.
var loadRates = func() map[string]float64 {
	return map[string]float64{"USD": 1, "EUR": 0.92, "GBP": 0.79}
}

// Rate returns the exchange rate from USD to currency.
func (s *Service) Rate(currency string) (float64, bool) {
	// ISSUE 10 [MAJOR] Lazy initialization with no synchronization. When
	// several goroutines call Rate at once, they all see nil, all call
	// loadRates (N API calls instead of one), and all write s.rates while
	// others read it: a data race. It's tempting to "fix" this by checking
	// nil outside a lock and locking only to assign ("double-checked
	// locking"), but that's still a race in Go: the unlocked read is
	// unsynchronized.
	// Fix: sync.Once, or sync.OnceValue (Go 1.21+):
	//   s.rates = sync.OnceValue(loadRates)   // in New
	//   r, ok := s.rates()[currency]          // in Rate
	// The first caller runs loadRates, concurrent callers wait for it, and
	// later calls return the cached result.
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
	// ISSUE 12 [MINOR] The cancel function is thrown away, so the context's
	// timer and resources live until the timeout fires, even when the
	// quote came back in 5ms. go vet reports "the cancel function ...
	// should be called, not discarded".
	// Fix: ctx, cancel := context.WithTimeout(...); defer cancel().
	ctx, _ = context.WithTimeout(ctx, 100*time.Millisecond)

	// ISSUE 11 [MAJOR] Goroutine leak. The channel is unbuffered, so the
	// goroutine's send blocks until someone receives. When the timeout
	// wins the select below, nobody ever receives: the goroutine is stuck
	// forever, holding its stack and everything it references. Every
	// timed-out quote leaks one goroutine, and in a busy service memory
	// grows until the process dies. Also, quoteCarrier doesn't take a ctx,
	// so even the slow call itself can't be told to stop.
	// Fix: make(chan Cents, 1). With room for one value, the send always
	// succeeds and the goroutine exits. And pass ctx into quoteCarrier so
	// the work stops when we stop waiting.
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

	// ISSUE 13 [BLOCKER] Ranges over s.orders and reads o.Status with no
	// lock, while Create may be adding orders and Charge may be updating
	// statuses (including the Charges this loop is starting!). Concurrent
	// map iteration and writes is a data race, and the runtime may kill
	// the process with "fatal error: concurrent map iteration and map
	// write", which recover can't catch.
	// Fix: under the lock, copy the pending IDs into a slice, unlock, then
	// loop over the slice.
	//
	// ISSUE 16 [MAJOR] Unbounded concurrency: one goroutine, and so one
	// simultaneous gateway request, per pending order. 50,000 pending
	// orders means 50,000 requests at once. The gateway will rate-limit or
	// ban you, and you may run out of sockets or memory.
	// Fix: bound it with a semaphore (a buffered channel with N slots), a
	// worker pool, or errgroup's SetLimit(N).
	for id, o := range s.orders {
		if o.Status != StatusPending {
			continue
		}
		wg.Add(1)
		// ISSUE 22 [NOT A BUG] "The goroutine captures the loop variable
		// id!" Before Go 1.22, all iterations shared ONE id variable, and
		// goroutines would mostly see whatever it held last. Since Go 1.22,
		// each iteration gets its own copy, so this is correct as long as
		// go.mod says go 1.22 or later (this repo says 1.25). Knowing when
		// a classic gotcha no longer applies, and checking go.mod before
		// flagging it, is a good interview signal.
		//
		// But look again: this same line DOES have a real bug. See ISSUE 14.
		go func() {
			s.chargeOne(ctx, id, wg, &succeeded)
		}()
	}

	wg.Wait()
	return succeeded
}

// ISSUE 14 [BLOCKER] wg is passed by VALUE. chargeOne gets its own copy of
// the WaitGroup, with the counter copied at call time, and calls Done on
// that copy. The original's counter never goes down, so wg.Wait in
// ChargeAll blocks forever. go vet reports it twice ("call of s.chargeOne
// copies lock value" and "chargeOne passes lock by value"). A WaitGroup,
// like a Mutex, must never be copied after first use.
// Fix: pass *sync.WaitGroup, or better, don't pass it at all:
// wg.Go(func() { ... }) (Go 1.25+) handles Add and Done for you.
//
// ISSUE 15 [BLOCKER] *succeeded++ runs in many goroutines at once. ++ is a
// read, an add, and a write, so concurrent increments overwrite each other
// and the count comes out too low. It's also a data race. (You'll only see
// this one once ISSUE 14 is fixed, because until then the test hangs
// first.)
// Fix: an atomic.Int64 (succeeded.Add(1)), or a mutex, or have each
// goroutine send its result on a channel and count in one place.
func (s *Service) chargeOne(ctx context.Context, id string, wg sync.WaitGroup, succeeded *int) {
	defer wg.Done()
	if err := s.Charge(ctx, id); err == nil {
		*succeeded++
	}
}

// WaitForStatus blocks until the order reaches want, or ctx is done.
func (s *Service) WaitForStatus(ctx context.Context, id string, want Status) error {
	// ISSUE 17 [MAJOR] A busy-wait. The empty default case makes the select
	// non-blocking, so this loop spins as fast as the CPU allows: one full
	// core at 100%, per waiting caller, doing nothing useful. And every
	// iteration takes s.mu (inside Get), so the spinners also slow down
	// everyone else who needs the lock. The test passes. This one is only
	// found by reading, or by noticing the CPU graph.
	// Fix: block until something actually changes. One pattern is a
	// "changed" channel guarded by s.mu, which is closed and replaced every
	// time any status changes: closing a channel wakes every goroutine
	// waiting on it. Waiters grab the current channel under the lock, then
	// select on it and ctx.Done(). (sync.Cond does the same job but can't
	// be combined with ctx.Done() in a select. A time.Ticker poll is a
	// simpler, cruder fallback.)
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
		if o.Status == want { // ISSUE 3 again: reading shared state through Get's pointer, with no lock
			return nil
		}
	}
}

// Close stops the background notifier.
func (s *Service) Close() {
	// ISSUE 19 [BLOCKER] Calling Close twice closes the channel twice, and
	// that panics. Shutdown code often runs more than once (a deferred
	// Close plus an explicit one, or a signal handler racing normal exit).
	// Fix: wrap it in a sync.Once so Close is idempotent.
	//
	// ISSUE 20 [BLOCKER] Charge, Create, Cancel, and Refund can all still
	// be running when Close is called, and a publish after this line
	// panics with "send on closed channel", crashing the process in the
	// middle of shutdown. The channel rule is "only the sender closes",
	// but here there are many senders and a different goroutine closing.
	// Fix: coordinate. Guard a `closed` flag with an RWMutex that publish
	// holds (shared) while sending; Close takes it exclusively, sets the
	// flag, and only then closes the channel. publish checks the flag and
	// drops or rejects events after Close.
	//
	// ISSUE 21 [MAJOR] Close returns right away, while notify may still be
	// working through buffered events. Anything that reads History or
	// Stats after Close (a shutdown report, a test) sees missing events,
	// and the notifier goroutine outlives the service. "Stop" should mean
	// "stopped".
	// Fix: notify closes a done channel when its loop ends (defer
	// close(done)), and Close waits on it: <-s.notifyDone.
	close(s.events)
}
