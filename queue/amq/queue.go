package amq

import (
	"crypto/tls"
	"net"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/syncmap"

	"time"

	"github.com/felipefoliatti/backoff"
	"github.com/felipefoliatti/errors"
	"github.com/go-stomp/stomp"

	"github.com/aws/aws-sdk-go/aws"
	uuid "github.com/satori/go.uuid"
)

// Message define uma mensagem lida da fila
// Uma mensagem possui Id e o próprio conteúdo em string
type Message struct {
	Content *string
	Handler interface{}
	Id      *string

	key       string    // broker message-id, used to identify the message across redeliveries
	gen       uint64    // subscription generation that delivered this message (its Handler is only valid on it)
	times     int       // how many times the message was postponed
	available time.Time // when the message may be returned again by Read
}

type Type int

const (
	SUBSCRIPTION Type = 1
	WRITE        Type = 2
)

const (
	// minDelay is the delay applied on the first Postpone; it doubles on each new Postpone
	minDelay = 10 * time.Second
	// maxDelay caps the exponential backoff
	maxDelay = time.Hour
	// idleWait is how long Read waits when there is nothing postponed (it is woken up by Postpone anyway)
	idleWait = time.Hour
	// historyTTL is how long the backoff state of a message is kept after its connection was lost,
	// waiting for the broker to redeliver it
	historyTTL = 2 * maxDelay
)

// Queue define um tipo que faz traca de mensagens
// Uma queue que pode ter diversas implementações
// Um tipo Queue poderá ser FIFO, onde há sua ordem garantida
type Queue interface {
	Send(content *string) (*string, *errors.Error)
	Read() ([]*Message, *errors.Error)
	Postpone(message *Message) *errors.Error
	Ack(message *Message) *errors.Error
	NAck(message *Message) *errors.Error
}

// pending keeps the backoff state of a postponed message whose connection was lost.
// The broker redelivers every unacked message on the new connection; when that happens,
// the message is put back in the cache with its original schedule instead of being processed right away.
type pending struct {
	times     int
	available time.Time
	lost      time.Time
}

// AmqQueue define uma implementação de uma Queue para a ActiveMQ
// Um tipo AmqQueue deve ser instanciado com o nome da Queue e internamente ele
// irá procurar pela URL ou, caso não exista, irá criar um tipo
type amqQueue struct {
	host        *string
	port        *string
	user        *string
	password    *string
	destination *string
	conn        syncmap.Map

	mu           sync.Mutex
	subscription *stomp.Subscription
	gen          uint64              // incremented on every new subscription
	messages     []*Message          // postponed messages (not acked), waiting to be returned again by Read
	history      map[string]*pending // backoff state of postponed messages lost on a reconnect, by message-id
	wake         chan struct{}       // signals Read that a message was postponed
	now          func() time.Time
}

// NewQueue creates a new AmqQueue
// If the Queue does not exists, it will be automatically created
func NewQueue(destination *string, host *string, port *string, user *string, password *string) (Queue, *errors.Error) {
	return newQueue(destination, host, port, user, password), nil
}

func newQueue(destination *string, host *string, port *string, user *string, password *string) *amqQueue {
	return &amqQueue{
		destination: destination,
		host:        host,
		port:        port,
		user:        user,
		password:    password,
		conn:        syncmap.Map{},
		messages:    []*Message{},
		history:     map[string]*pending{},
		wake:        make(chan struct{}, 1),
		now:         time.Now,
	}
}

func (q *amqQueue) connect(t Type) *errors.Error {

	var err *errors.Error
	if strings.Contains(*q.host, "https") {

		dialer := net.Dialer{Timeout: time.Minute}
		dial, e := tls.DialWithDialer(&dialer, "tcp", strings.Replace(*q.host, "https://", "", -1)+":"+*q.port, &tls.Config{})

		err = errors.WrapInner("error opening the connection to queue via HTTPS", e, 0)
		//If any error, stops
		if err != nil {
			return err
		}

		var conn *stomp.Conn
		connected := make(chan bool)

		//sometimes, if the remote server is not alive or tcp connection stops, the connection is closed - when try to reconnect - it stucks forever - here we wait two minutes to connect
		go func() {
			var e error
			conn, e = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
			err = errors.WrapInner("error connecting to queue via STOMP", e, 0)
			connected <- err == nil //check if success
		}()

		var success *bool
		for success == nil {
			select {
			case s := <-connected:
				success = &s //put the information here
			case <-time.After(2 * time.Minute):
				dial.Close() //cloce all the shit - when close, release the io.Reader.Read from the goroutine above
			}
		}

		//If any error, stops
		if err != nil {
			return err
		}

		q.conn.Store(t, conn)

	} else {

		dialer := net.Dialer{Timeout: time.Minute}
		dial, e := dialer.Dial("tcp", strings.Replace(*q.host, "http://", "", -1)+":"+*q.port)

		err = errors.WrapInner("error opening the connection to queue via HTTP", e, 0)

		//If any error, stops
		if err != nil {
			return err
		}

		var conn *stomp.Conn
		connected := make(chan bool)

		//sometimes, if the remote server is not alive or tcp connection stops, the connection is closed - when try to reconnect - it stucks forever - here we wait two minutes to connect
		go func() {
			var e error
			conn, e = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
			err = errors.WrapInner("error opening the connection to queue", e, 0)
			connected <- err == nil //check if success
		}()

		var success *bool
		for success == nil {
			select {
			case s := <-connected:
				success = &s //put the information here
			case <-time.After(2 * time.Minute):
				dial.Close() //cloce all the shit  - when close, release the io.Reader.Read from the goroutine above
			}
		}

		//If any error, stops
		if err != nil {
			return err
		}

		q.conn.Store(t, conn)

	}

	return nil
}

// Send is the implementation that is responsible to send message to queue
// In the return, a GUID is returned, the GUID is a Correlation-ID
// If any error happen, the error object is returned
func (q *amqQueue) Send(content *string) (*string, *errors.Error) {

	var err *errors.Error
	var e error
	guid := uuid.NewV4()

	e = backoff.Retry(func() error {

		err = nil
		if _, ok := q.conn.Load(WRITE); !ok {
			err = q.connect(WRITE)
		}

		if err == nil {

			obj, _ := q.conn.Load(WRITE)
			queue := obj.(*stomp.Conn)

			e = queue.Send(*q.destination, "application/json", []byte(*content), stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", "true"), stomp.SendOpt.Header("correlation-id", guid.String()))
			err = errors.WrapInner("failed to send to queue: retrying to open a connection", e, 0)

			if err != nil {
				queue.Disconnect()
				q.conn.Delete(WRITE)
				return err
			}
			return nil
		}

		return err

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	//if any error, don't return the uuid
	if e != nil {
		return nil, errors.WrapInner("error sending the message", e, 0)
	}

	return aws.String(guid.String()), nil
}

// Read realize a blocking read, if there is any message, it returns,
// The method can also return an empty list
// If any error happen, the error object is returned
//
// Postponed messages are returned again when their backoff expires. They have priority
// over new messages, so they are not starved when the queue is busy.
func (q *amqQueue) Read() ([]*Message, *errors.Error) {

	failures := 0

	for {
		//1. a postponed message is due: return it before waiting for new ones
		if m := q.popDue(); m != nil {
			return []*Message{m}, nil
		}

		//2. make sure there is a subscription
		sub, err := q.subscribe()
		if err != nil {
			return nil, err
		}

		//3. wait for a new message, the next postponed one or a new Postpone
		timer := time.NewTimer(q.nextWait())
		select {
		case nMsg, ok := <-sub.C:
			timer.Stop()

			if !ok || nMsg == nil || nMsg.Err != nil {
				q.resetSubscription(sub)

				var cause interface{} = "subscription closed"
				if ok && nMsg != nil {
					cause = nMsg.Err
				}

				failures++
				if failures >= 3 {
					return nil, errors.WrapInner("failed to listen to queue", cause, 0)
				}
				time.Sleep(time.Duration(failures) * time.Second)
				continue
			}

			if m := q.accept(nMsg, sub); m != nil {
				return []*Message{m}, nil
			}
			//the message was a redelivery of a postponed message still in backoff: keep waiting

		case <-timer.C:
		case <-q.wake:
			timer.Stop()
		}
	}
}

// subscribe returns the current subscription or creates a new one (opening the connection if needed)
func (q *amqQueue) subscribe() (*stomp.Subscription, *errors.Error) {

	q.mu.Lock()
	sub := q.subscription
	q.mu.Unlock()

	if sub != nil {
		return sub, nil
	}

	var err *errors.Error
	e := backoff.Retry(func() error {

		if _, ok := q.conn.Load(SUBSCRIPTION); !ok {
			if err = q.connect(SUBSCRIPTION); err != nil {
				return err
			}
		}

		obj, _ := q.conn.Load(SUBSCRIPTION)
		conn := obj.(*stomp.Conn)

		s, e := conn.Subscribe(*q.destination, stomp.AckClientIndividual)
		if err = errors.WrapInner("error subscribing to queue", e, 0); err != nil {
			//if any error subscribing, clear up everything
			conn.Disconnect()
			q.conn.Delete(SUBSCRIPTION)
			return err
		}

		q.mu.Lock()
		q.subscription = s
		q.gen++
		q.mu.Unlock()

		sub = s
		return nil

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if e != nil {
		return nil, errors.WrapInner("error opening the subscription", e, 0)
	}

	return sub, nil
}

// resetSubscription drops the subscription and its connection.
// The broker will redeliver every unacked (postponed) message on the next connection, and the
// handlers of the cached messages are no longer valid, so the cache is moved to the history,
// keeping the backoff state of each message until the broker redelivers it.
func (q *amqQueue) resetSubscription(sub *stomp.Subscription) {

	if !q.detach(sub) {
		return
	}

	sub.Unsubscribe()

	if obj, ok := q.conn.Load(SUBSCRIPTION); ok {
		obj.(*stomp.Conn).Disconnect()
		q.conn.Delete(SUBSCRIPTION)
	}
}

// detach forgets the subscription and moves the cached messages to the history.
// It returns false if sub is not the current subscription (already detached).
func (q *amqQueue) detach(sub *stomp.Subscription) bool {

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.subscription != sub {
		return false
	}
	q.subscription = nil

	now := q.now()
	for _, m := range q.messages {
		q.history[m.key] = &pending{times: m.times, available: m.available, lost: now}
	}
	q.messages = []*Message{}
	return true
}

// accept converts a message received from the broker. If it is a redelivery of a message that
// is still in backoff (lost on a reconnect), it is put back in the cache and nil is returned.
func (q *amqQueue) accept(nMsg *stomp.Message, sub *stomp.Subscription) *Message {

	q.mu.Lock()
	defer q.mu.Unlock()

	m := &Message{
		Id:        aws.String(nMsg.Header.Get("correlation-id")),
		Content:   aws.String(string(nMsg.Body)),
		Handler:   nMsg,
		key:       nMsg.Header.Get("message-id"),
		available: q.now(),
	}
	if q.subscription == sub {
		m.gen = q.gen
	}

	//remove any cached copy of the same message (its handler belongs to an old delivery)
	for i := 0; i < len(q.messages); i++ {
		if q.messages[i].key == m.key {
			q.messages = append(q.messages[:i], q.messages[i+1:]...)
			i--
		}
	}

	if p, ok := q.history[m.key]; ok {
		delete(q.history, m.key)
		m.times = p.times
		m.available = p.available

		if m.available.After(q.now()) {
			q.messages = append(q.messages, m)
			return nil
		}
	}

	return m
}

// popDue removes and returns the postponed message with the earliest expired backoff, if any
func (q *amqQueue) popDue() *Message {

	q.mu.Lock()
	defer q.mu.Unlock()

	now := q.now()

	//forget the backoff of lost messages that were never redelivered (consumed by another consumer, expired...)
	for k, p := range q.history {
		if now.Sub(p.lost) > historyTTL {
			delete(q.history, k)
		}
	}

	if len(q.messages) == 0 {
		return nil
	}

	sort.SliceStable(q.messages, func(i, j int) bool {
		return q.messages[i].available.Before(q.messages[j].available)
	})

	if m := q.messages[0]; !m.available.After(now) {
		q.messages = q.messages[1:]
		return m
	}
	return nil
}

// nextWait returns how long Read may block before the next postponed message is due
func (q *amqQueue) nextWait() time.Duration {

	q.mu.Lock()
	defer q.mu.Unlock()

	wait := idleWait
	now := q.now()
	for _, m := range q.messages {
		if until := m.available.Sub(now); until < wait {
			wait = until
		}
	}
	if wait < 0 {
		wait = 0
	}
	return wait
}

// delay returns the backoff for a message postponed n times (n >= 1): 10s, 20s, 40s ... up to 1h
func delay(n int) time.Duration {
	d := minDelay
	for i := 1; i < n && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d
}

// Delete/Ack the message
func (q *amqQueue) Ack(handle *Message) *errors.Error {
	return q.ackNack(handle, true)
}

func (q *amqQueue) NAck(handle *Message) *errors.Error {
	return q.ackNack(handle, false)
}

func (q *amqQueue) ackNack(handle *Message, ack bool) *errors.Error {

	msg, ok := handle.Handler.(*stomp.Message)
	if !ok {
		return errors.WrapInner("invalid message", "the message handler is not a stomp message", 0)
	}

	q.mu.Lock()
	valid := q.subscription != nil && handle.gen == q.gen
	q.mu.Unlock()

	//the connection that delivered the message was closed: the broker will redeliver it
	if !valid {
		return errors.WrapInner("error ack'ing the message", "the connection that delivered the message was closed, the message will be redelivered", 0)
	}

	obj, ok := q.conn.Load(SUBSCRIPTION)
	if !ok {
		return errors.WrapInner("error ack'ing the message", "there is no connection to the queue", 0)
	}
	conn := obj.(*stomp.Conn)

	if ack {
		return errors.WrapInner("error ack'ing the message", conn.Ack(msg), 0)
	}
	return errors.WrapInner("error nack'ing the message", conn.Nack(msg), 0)
}

// Postpone keeps the message (without ack) to be returned again by Read after an exponential
// backoff: 10s, 20s, 40s ... up to 1h. Safe to call from other goroutines.
func (q *amqQueue) Postpone(msg *Message) *errors.Error {

	q.mu.Lock()

	msg.times++
	msg.available = q.now().Add(delay(msg.times))

	if q.subscription != nil && msg.gen == q.gen {
		found := false
		for _, m := range q.messages {
			if m == msg {
				found = true
				break
			}
		}
		if !found {
			q.messages = append(q.messages, msg)
		}
	} else {
		//the connection was lost: the broker will redeliver it, keep only the backoff state
		q.history[msg.key] = &pending{times: msg.times, available: msg.available, lost: q.now()}
	}

	q.mu.Unlock()

	//wake Read up so it recalculates how long to wait
	select {
	case q.wake <- struct{}{}:
	default:
	}

	return nil
}
