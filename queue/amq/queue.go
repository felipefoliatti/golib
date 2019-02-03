package amq

import (
	"crypto/tls"
	"math"
	"net"
	"strings"
	"time"

	"github.com/felipefoliatti/backoff"
	"github.com/felipefoliatti/errors"

	"github.com/aws/aws-sdk-go/aws"
	uuid "github.com/satori/go.uuid"

	stomp "github.com/go-stomp/stomp"
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
	conn         *stomp.Conn
	subscription *stomp.Subscription
	messages     []*Message
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

	err := self.connect()

	return self, err
}

func (q *amqQueue) connect() *errors.Error {
	var err *errors.Error
	if strings.Contains(*q.host, "https") {

		dial, e := tls.Dial("tcp", strings.Replace(*q.host, "https://", "", -1)+":"+*q.port, &tls.Config{})
		err = errors.WrapPrefix(e, "error opening the connection to queue via HTTPS", 0)
		//If any error, stops
		if err != nil {
			return err
		}

		q.conn, e = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
		err = errors.WrapPrefix(e, "error connecting to queue via STOMP", 0)

		//If any error, stops
		if err != nil {
			return err
		}

	} else {

		dial, e := net.Dial("tcp", strings.Replace(*q.host, "http://", "", -1)+":"+*q.port)
		err = errors.WrapPrefix(e, "error opening the connection to queue via HTTP", 0)

		//If any error, stops
		if err != nil {
			return err
		}

		q.conn, e = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
		err = errors.WrapPrefix(e, "error opening the connection to queue", 0)

		//If any error, stops
		if err != nil {
			return err
		}
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

		if q.conn == nil {
			q.connect()
		}

		e = q.conn.Send(*q.destination, "application/json", []byte(*content), stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", "true"), stomp.SendOpt.Header("correlation-id", guid.String()))
		err = errors.WrapPrefix(e, "failed to listen to topic: retrying to open a connection", 0)

		if err != nil {
			q.conn.Disconnect()
			q.conn = nil
			return err
		}

		return nil

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
	var msg *stomp.Message

	//check if is any message to return
	for i, m := range q.messages {

		if m.available.Before(time.Now()) {
			m.available = m.available.Add(time.Duration(math.Pow(2, float64(m.times))) * time.Second)
			m.times++
			//remove from list
			q.messages = append(q.messages[:i], q.messages[i+1:]...)

			//if found, return it first
			return []*Message{m}, nil
		}

	}

	e = backoff.Retry(func() error {

		if q.conn == nil {
			q.connect()
		}

		if q.subscription == nil {

			//Create the subscription
			q.subscription, e = q.conn.Subscribe(*q.destination, stomp.AckClientIndividual)
			err = errors.WrapPrefix(e, "error subscribing to queue", 0)

			//If any error, stops
			if err != nil {
				return err
			}
		}

		msg = <-q.subscription.C

		if msg == nil {
			if q.subscription != nil {
				q.subscription.Unsubscribe()
			}

			q.conn.Disconnect()
			q.conn = nil
			q.subscription = nil

			return errors.New("failed to listen to topic: retrying to open a connection")
		}

		return nil

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	if err != nil {
		return nil, e.(*errors.Error)
	}

	if msg.Err != nil {
		return nil, errors.WrapPrefix(msg.Err, "failed to listen to topic", 0)
	}

	messages := make([]*Message, 1)
	messages[0] = &Message{Id: aws.String(msg.Header.Get("correlation-id")), Content: aws.String(string(msg.Body)), Handler: msg, times: 1, available: time.Now()}

	return messages, nil
}

// Delete/Ack the message
func (q *amqQueue) Ack(handle *Message) *errors.Error {
	msg := handle.Handler.(*stomp.Message)
	e := q.conn.Ack(msg)
	return errors.WrapPrefix(e, "error ack'ing the message", 0)
}
func (q *amqQueue) NAck(handle *Message) *errors.Error {
	msg := handle.Handler.(*stomp.Message)
	e := q.conn.Nack(msg)
	return errors.WrapPrefix(e, "error nack'ing the message", 0)
}

func (q *amqQueue) Postpone(msg *Message) *errors.Error {
	q.messages = append(q.messages, msg)
	return nil
}
