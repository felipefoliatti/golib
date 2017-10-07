package golib

import (
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/satori/go.uuid"
)

// Message define uma mensagem lida da fila
// Uma mensagem possui Id e o próprio conteúdo em string
type Message struct {
	Content *string
	Handler *string
	Id      *string
}

// Queue define um tipo que faz traca de mensagens
// Uma queue que pode ter diversas implementações
// Um tipo Queue poderá ser FIFO, onde há sua ordem garantida
type Queue interface {
	Send(content *string) (*string, error)
	Read() ([]*Message, error)
	Delete(handle *string) error
}

// SqsQueue define uma implementação de uma Queue para a Amazon AWS
// Um tipo SqsQueue deve ser instanciado com o nome da Queue e internamente ele
// irá procurar pela URL ou, caso não exista, irá criar um tipo
type sqsQueue struct {
	endpoint   *string
	region     *string
	accessKey  *string
	secret     *string
	name       *string
	url        *string
	svc        *sqs.SQS
	visibility int
	fifo       bool
}

// NewQueue inicia uma nova SqsQueue a partir de um nome
// Se a fila não existir, ela será criada
// Caso exista, apenas será retornada
func NewQueue(name *string, sufix *string, endpoint *string, accessKey *string, secret *string, region *string, visibility int) (Queue, error) {
	self := &sqsQueue{}
	self.accessKey = accessKey
	self.secret = secret
	self.region = region
	self.endpoint = endpoint
	self.visibility = visibility
	self.name = aws.String(*name + *sufix)
	self.fifo = (*sufix == ".fifo")
	token := ""

	creds := credentials.NewStaticCredentials(*accessKey, *secret, token)
	_, err := creds.Get()

	if err != nil {
		return nil, err
	}

	config := &aws.Config{Region: region, Endpoint: self.endpoint, Credentials: creds}

	sess := session.New(config)
	svc := sqs.New(sess)

	//Cria a queue se não existir
	params := &sqs.GetQueueUrlInput{QueueName: name}
	resp, err := svc.GetQueueUrl(params)

	if err == nil {
		self.url = resp.QueueUrl
	} else {
		//Verifica se é um erro de não existir
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == sqs.ErrCodeQueueDoesNotExist {

			//Caso não exista, então cria a queue
			attr := map[string]*string{}

			attr["VisibilityTimeout"] = aws.String(strconv.Itoa(visibility))
			if self.fifo { //only add FifoQueue if queue ends with .fifo
				attr["FifoQueue"] = aws.String(strconv.FormatBool(self.fifo))
			}

			params := &sqs.CreateQueueInput{QueueName: self.name, Attributes: attr}
			resp, err := svc.CreateQueue(params)

			//Caso haja erro, encerra
			if err != nil {
				return nil, err
			}
			self.url = resp.QueueUrl
		}
	}
	self.svc = svc

	return self, nil
}

// Send é a implementação para uma SqsQueue que envia uma mensagem
// Como retorno, ele retorna um GUID, indicando a identificação da mensagem enviada
// Caso haja algum erro, então um objeto error é retornado
func (q *sqsQueue) Send(content *string) (*string, error) {

	guid := uuid.NewV4()
	params := &sqs.SendMessageInput{
		MessageBody: content,
		QueueUrl:    q.url,
	}

	//Only Fifo can have these attributes
	if q.fifo {
		params.SetMessageDeduplicationId(guid.String())
		params.SetMessageGroupId("base")
	}

	req, err := q.svc.SendMessage(params)

	if err != nil {
		return nil, err
	}

	return req.MessageId, nil
}

// Read realiza uma leitura bloqueante na SqsQueue, de modo que, se houve uma mensagem, ela retorna,
// caso contrário, retorna uma lista vazia de mensagens
// Se houver algum erro, é retornado no objeto error
func (q *sqsQueue) Read() ([]*Message, error) {
	result, err := q.svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames:        []*string{aws.String(sqs.MessageSystemAttributeNameSentTimestamp)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		QueueUrl:              q.url,
		MaxNumberOfMessages:   aws.Int64(1),
		VisibilityTimeout:     aws.Int64(int64(q.visibility)),
		WaitTimeSeconds:       aws.Int64(20),
	})

	if err != nil {
		return nil, err
	}

	messages := make([]*Message, len(result.Messages))
	for i := range result.Messages {
		messages[i] = &Message{Id: result.Messages[i].MessageId, Content: result.Messages[i].Body, Handler: result.Messages[i].ReceiptHandle}
	}

	return messages, nil
}

// Delete remove uma mensagem baseada em seu handle, que é uma string
func (q *sqsQueue) Delete(handle *string) error {
	_, err := q.svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      q.url,
		ReceiptHandle: handle,
	})
	return err
}
