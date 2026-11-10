package amq

import (
	"testing"
	"time"

	"github.com/go-stomp/stomp"
	"github.com/go-stomp/stomp/frame"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }
func dest() *string                  { s := "/queue/test"; return &s }
func stompMsg(id string) *stomp.Message {
	return &stomp.Message{Header: frame.NewHeader("message-id", id, "correlation-id", "c-"+id), Body: []byte("body-" + id)}
}

// newTestQueue returns a queue with a fake clock and a (fake) active subscription
func newTestQueue() (*amqQueue, *clock, *stomp.Subscription) {
	c := &clock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	q := newQueue(dest(), nil, nil, nil, nil)
	q.now = c.now
	sub := &stomp.Subscription{}
	q.subscription = sub
	q.gen = 1
	return q, c, sub
}

func TestDelayIsExponentialAndCapped(t *testing.T) {
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second, 320 * time.Second, 640 * time.Second, 1280 * time.Second, 2560 * time.Second, time.Hour, time.Hour}
	for i, w := range want {
		if d := delay(i + 1); d != w {
			t.Errorf("delay(%d) = %v, want %v", i+1, d, w)
		}
	}
	if d := delay(1000); d != time.Hour {
		t.Errorf("delay(1000) = %v, want 1h", d)
	}
}

func TestPostponeBackoffGrows(t *testing.T) {
	q, c, sub := newTestQueue()

	m := q.accept(stompMsg("1"), sub)
	if m == nil {
		t.Fatal("new message must be returned")
	}

	for i := 1; i <= 4; i++ {
		q.Postpone(m)
		d := delay(i)

		if got := q.nextWait(); got != d {
			t.Fatalf("postpone #%d: nextWait = %v, want %v", i, got, d)
		}
		c.add(d - time.Second)
		if q.popDue() != nil {
			t.Fatalf("postpone #%d: message returned before its backoff", i)
		}
		c.add(time.Second)
		if got := q.popDue(); got != m {
			t.Fatalf("postpone #%d: message not returned after its backoff", i)
		}
	}
}

func TestPostponedMessageIsNotStarvedByNewMessages(t *testing.T) {
	q, c, sub := newTestQueue()

	m := q.accept(stompMsg("1"), sub)
	q.Postpone(m)

	//keep receiving new messages for longer than the backoff
	for i := 0; i < 120; i++ {
		q.accept(stompMsg("n"), sub)
		c.add(time.Second)
	}

	//Read checks the cache before waiting for new messages
	if got := q.popDue(); got != m {
		t.Fatal("postponed message should be due and returned")
	}
}

func TestPostponeWakesRead(t *testing.T) {
	q, _, sub := newTestQueue()
	q.Postpone(q.accept(stompMsg("1"), sub))

	select {
	case <-q.wake:
	default:
		t.Fatal("Postpone must signal Read")
	}
}

func TestPostponeTwiceDoesNotDuplicate(t *testing.T) {
	q, _, sub := newTestQueue()
	m := q.accept(stompMsg("1"), sub)
	q.Postpone(m)
	q.Postpone(m)
	if len(q.messages) != 1 {
		t.Fatalf("cache has %d messages, want 1", len(q.messages))
	}
}

func TestReconnectKeepsBackoff(t *testing.T) {
	q, c, sub := newTestQueue()

	m := q.accept(stompMsg("1"), sub)
	q.Postpone(m)
	q.Postpone(m) //times=2, available in 20 seconds

	//connection lost
	if !q.detach(sub) {
		t.Fatal("detach should succeed")
	}
	if len(q.messages) != 0 || len(q.history) != 1 {
		t.Fatalf("cache must move to history: messages=%d history=%d", len(q.messages), len(q.history))
	}

	//old handler cannot be acked on the new connection
	if err := q.Ack(m); err == nil {
		t.Fatal("ack of a message from a lost connection must fail")
	}

	//new subscription; the broker redelivers the unacked message right away
	sub2 := &stomp.Subscription{}
	q.subscription = sub2
	q.gen++

	if got := q.accept(stompMsg("1"), sub2); got != nil {
		t.Fatal("redelivered message still in backoff must not be returned")
	}
	if len(q.messages) != 1 || len(q.history) != 0 {
		t.Fatalf("redelivery must go back to the cache: messages=%d history=%d", len(q.messages), len(q.history))
	}

	c.add(2 * time.Minute)
	got := q.popDue()
	if got == nil {
		t.Fatal("message should be due after the original backoff")
	}
	if got.times != 2 || got.gen != q.gen || got.Handler == m.Handler {
		t.Fatalf("message must keep its retry count and use the new handler: times=%d gen=%d", got.times, got.gen)
	}

	//next postpone continues the backoff
	q.Postpone(got)
	if w := q.nextWait(); w != 40*time.Second {
		t.Fatalf("nextWait = %v, want 40s", w)
	}
}

func TestRedeliveryAlreadyDueIsReturned(t *testing.T) {
	q, c, sub := newTestQueue()
	m := q.accept(stompMsg("1"), sub)
	q.Postpone(m)
	q.detach(sub)

	c.add(5 * time.Minute)
	sub2 := &stomp.Subscription{}
	q.subscription = sub2
	q.gen++

	if got := q.accept(stompMsg("1"), sub2); got == nil || got.times != 1 {
		t.Fatal("redelivered message past its backoff must be returned with its retry count")
	}
}

func TestPostponeAfterConnectionLostKeepsOnlyBackoff(t *testing.T) {
	q, _, sub := newTestQueue()
	m := q.accept(stompMsg("1"), sub)
	q.detach(sub) //connection lost while the message was being processed

	q.Postpone(m)
	if len(q.messages) != 0 {
		t.Fatal("message with a dead handler must not go to the cache")
	}
	if p := q.history["1"]; p == nil || p.times != 1 {
		t.Fatal("backoff must be kept in the history")
	}
}

func TestHistoryExpires(t *testing.T) {
	q, c, sub := newTestQueue()
	q.Postpone(q.accept(stompMsg("1"), sub))
	q.detach(sub)

	c.add(historyTTL + time.Second)
	q.popDue()
	if len(q.history) != 0 {
		t.Fatal("history must expire")
	}
}

func TestPopDueReturnsEarliest(t *testing.T) {
	q, c, sub := newTestQueue()
	a := q.accept(stompMsg("a"), sub)
	b := q.accept(stompMsg("b"), sub)
	q.Postpone(a)
	q.Postpone(a) //20s
	q.Postpone(b) //10s

	c.add(3 * time.Minute)
	if q.popDue() != b || q.popDue() != a || q.popDue() != nil {
		t.Fatal("due messages must be returned earliest first")
	}
}
