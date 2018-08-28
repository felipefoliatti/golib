package amq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/satori/go.uuid"

	stomp "github.com/go-stomp/stomp"
)

// Message define uma mensagem lida da fila
// Uma mensagem possui Id e o próprio conteúdo em string
type Message struct {
	Content *string
	Handler interface{}
	Id      *string
}

// Queue define um tipo que faz traca de mensagens
// Uma queue que pode ter diversas implementações
// Um tipo Queue poderá ser FIFO, onde há sua ordem garantida
type Queue interface {
	Send(content *string) (*string, error)
	Read() ([]*Message, error)
	Ack(handle interface{}) error
	NAck(handle interface{}) error
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
}

// NewQueue creates a new AmqQueue
// If the Queue does not exists, it will be automatically created
func NewQueue(destination *string, host *string, port *string, user *string, password *string) (Queue, error) {
	self := &amqQueue{}
	self.destination = destination
	self.host = host
	self.port = port
	self.user = user
	self.password = password

	err := self.connect()

	return self, err
}

func (q *amqQueue) connect() error {

	if strings.Contains(*q.host, "https") {

		dial, err := tls.Dial("tcp", strings.Replace(*q.host, "https://", "", -1)+":"+*q.port, &tls.Config{})
		//If any error, stops
		if err != nil {
			return err
		}

		q.conn, err = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
		//If any error, stops
		if err != nil {
			return err
		}

	} else {

		dial, err := net.Dial("tcp", strings.Replace(*q.host, "http://", "", -1)+":"+*q.port)
		//If any error, stops
		if err != nil {
			return err
		}

		q.conn, err = stomp.Connect(dial, stomp.ConnOpt.Login(*q.user, *q.password), stomp.ConnOpt.HeartBeatError(60*time.Second), stomp.ConnOpt.WriteChannelCapacity(20000), stomp.ConnOpt.ReadChannelCapacity(20000))
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
func (q *amqQueue) Send(content *string) (*string, error) {

	guid, err := uuid.NewV4()
	err = backoff.Retry(func() error {

		if q.conn == nil {
			q.connect()
		}

		err = q.conn.Send(*q.destination, "application/json", []byte(*content), stomp.SendOpt.Receipt, stomp.SendOpt.Header("persistent", "true"), stomp.SendOpt.Header("correlation-id", guid.String()))

		if err != nil {
			q.conn.Disconnect()
			q.conn = nil
			return errors.New(fmt.Sprint("failed to listen to topic: retrying to open a connection: ", err.Error()))
		}

		return nil

	}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3))

	//if any error, don't return the uuid
	if err != nil {
		return nil, err
	}

	return aws.String(guid.String()), nil
}

// Read realize a blocking read, if there is any message, it returns,
// The method can also return an empty list
// If any error happen, the error object is returned
func (q *amqQueue) Read() ([]*Message, error) {

	var msg *stomp.Message
	err := backoff.Retry(func() error {

		if q.conn == nil {
			q.connect()
		}

		if q.subscription == nil {
			var e error
			//Create the subscription
			q.subscription, e = q.conn.Subscribe(*q.destination, stomp.AckClient)
			//If any error, stops
			if e != nil {
				return e
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
		return nil, err
	}
	if msg.Err != nil {
		return nil, errors.New("failed to listen to topic: " + msg.Err.Error())
	}

	messages := make([]*Message, 1)
	messages[0] = &Message{Id: aws.String(msg.Header.Get("correlation-id")), Content: aws.String(string(msg.Body)), Handler: msg}

	return messages, nil
}

// Delete/Ack the message
func (q *amqQueue) Ack(handle interface{}) error {
	msg := handle.(*stomp.Message)
	return q.conn.Ack(msg)
}
func (q *amqQueue) NAck(handle interface{}) error {
	msg := handle.(*stomp.Message)
	return q.conn.Nack(msg)
}
