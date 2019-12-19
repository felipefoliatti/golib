package amq

import (
	"crypto/tls"
	"math"
	"net"
	"strings"
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

	times     int
	available time.Time
}

type Type int

const (
	SUBSCRIPTION Type = 1
	WRITE        Type = 2
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

// AmqQueue define uma implementação de uma Queue para a ActiveMQ
// Um tipo AmqQueue deve ser instanciado com o nome da Queue e internamente ele
// irá procurar pela URL ou, caso não exista, irá criar um tipo
type amqQueue struct {
	host         *string
	port         *string
	user         *string
	password     *string
	destination  *string
	conn         map[Type]*stomp.Conn
	subscription *stomp.Subscription
	messages     []*Message
	wait         time.Duration
}

// NewQueue creates a new AmqQueue
// If the Queue does not exists, it will be automatically created
func NewQueue(destination *string, host *string, port *string, user *string, password *string) (Queue, *errors.Error) {
	self := &amqQueue{}
	self.destination = destination
	self.host = host
	self.port = port
	self.user = user
	self.password = password
	self.messages = []*Message{}
	self.wait = time.Hour //1 hour

	self.conn = map[Type]*stomp.Conn{}
	//err := self.connect()

	return self, nil
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

		q.conn[t] = conn

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

		q.conn[t] = conn

	}

	return nil
}

// Send is the implementation that is responsible to send message to queue
// In the return, a GUID is returned, the GUID is a Correlation-ID
// If any error happen, the error object is returned
// Send is the implementation that is responsible to send message to queue
// In the return, a GUID is returned, the GUID is a Correlation-ID
// If any error happen, the error object is returned
func (q *amqQueue) Send(content *string) (*string, *errors.Error) {

	var err *errors.Error
	var e error
	guid, e := uuid.NewV4()

	e = backoff.Retry(func() error {

		if q.conn[WRITE] == nil {
			err = q.connect(WRITE)
		}

		if err == nil {

			e = q.conn[WRITE].Send(*q.destination, "application/json", []byte(*content), stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", "true"), stomp.SendOpt.Header("correlation-id", guid.String()))
			err = errors.WrapInner("failed to listen to topic: retrying to open a connection", e, 0)

			if err != nil {
				q.conn[WRITE].Disconnect()
				q.conn[WRITE] = nil
				return err
			}
		}

		return err

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	//if any error, don't return the uuid
	if e != nil {
		return nil, e.(*errors.Error)
	}

	return aws.String(guid.String()), nil
}

// Read realize a blocking read, if there is any message, it returns,
// The method can also return an empty list
// If any error happen, the error object is returned
func (q *amqQueue) Read() ([]*Message, *errors.Error) {

	var e error
	var err *errors.Error

	var nMsg *stomp.Message //new message read from queue
	var oMsg *Message       //old message to process again

	e = backoff.Retry(func() error {

		if q.conn[SUBSCRIPTION] == nil {
			e = q.connect(SUBSCRIPTION)
			if e != nil {
				return e
			}
		}

		if q.subscription == nil && q.conn[SUBSCRIPTION] != nil {

			//Create the subscription
			q.subscription, e = q.conn[SUBSCRIPTION].Subscribe(*q.destination, stomp.AckClientIndividual)
			err = errors.WrapInner("error subscribing to queue", e, 0)

			//If any error, stops
			if err != nil {
				return err
			}
		}

	loop:
		for {
			select {
			case nMsg = <-q.subscription.C:
				break loop //stop the for loop
			case <-time.After(q.wait):
				//check if is any message to return
				for i, m := range q.messages {
					if m.available.Before(time.Now()) {
						m.available = time.Now().Add(time.Duration(math.Pow(2, float64(m.times))) * time.Second)

						m.times++
						m.times = int(math.Max(float64(m.times), 12)) //at most, 1h

						//remove from list
						q.messages = append(q.messages[:i], q.messages[i+1:]...)

						//if found, stop the inner loop (but does not break the inner most loop, cause we have to calculate the min time to wait first)
						oMsg = m
						break
					}
				}

				//create a new temp array to check how much (at least must to wait)
				temp := q.messages
				if oMsg != nil {
					temp = append(temp, oMsg)
				}

				//check the next
				q.wait = time.Hour
				for _, m := range temp {
					since := time.Since(m.available)
					if since < q.wait {
						q.wait = since
					}
				}

				//if the message was found inside the for loop
				if oMsg != nil {
					break loop
				}
			}
		}

		if nMsg != nil && nMsg.Err != nil {
			if q.subscription != nil {
				q.subscription.Unsubscribe()
			}

			q.conn[SUBSCRIPTION].Disconnect()
			q.conn[SUBSCRIPTION] = nil
			q.subscription = nil

			return errors.WrapInner("failed to listen to queue: retrying to open a connection", nMsg.Err, 0)
		}

		return nil

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if err != nil {
		return nil, e.(*errors.Error)
	}

	//if any error from message read from queue - if any message from queue
	if nMsg != nil && nMsg.Err != nil {
		return nil, errors.WrapInner("failed to listen to queue", nMsg.Err, 0)
	}

	messages := []*Message{}

	if nMsg != nil {
		//If any new message from queue
		messages = append(messages, &Message{Id: aws.String(nMsg.Header.Get("correlation-id")), Content: aws.String(string(nMsg.Body)), Handler: nMsg, times: 1, available: time.Now()})
		//check to see if the message is inside the messages (if the broker resend it - if find, then remove it)
		for i, m := range q.messages {
			if m.Id == messages[0].Id {
				q.messages = append(q.messages[:i], q.messages[i+1:]...)
			}
		}
	} else if oMsg != nil {
		//If any old message to try again
		messages = append(messages, oMsg)
	}

	return messages, nil
}

// Delete/Ack the message
func (q *amqQueue) Ack(handle *Message) *errors.Error {
	msg := handle.Handler.(*stomp.Message)
	e := q.conn[SUBSCRIPTION].Ack(msg)
	return errors.WrapInner("error ack'ing the message", e, 0)
}
func (q *amqQueue) NAck(handle *Message) *errors.Error {
	msg := handle.Handler.(*stomp.Message)
	e := q.conn[SUBSCRIPTION].Nack(msg)
	return errors.WrapInner("error nack'ing the message", e, 0)
}

func (q *amqQueue) Postpone(msg *Message) *errors.Error {
	q.messages = append(q.messages, msg)
	q.wait = time.Second
	return nil
}
