package golib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

type Log struct {
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
	Location  string `json:"location"`
	Level     string `json:"level"`
	Container string `json:"container"`
	Message   string `json:"message"`
}

//Logger descreve uma interface de log de ero, que loga em diversas fontes, conforme suas implementações
type Logger interface {
	LogIf(log bool, level string, info string, message func() string, action func())
}

//LoggerSQS define uma implementação de Logger para logar em filas SQS da Amazon, além do console
type LoggerSQS struct {
	queue    Queue
	ttype    string
	location string
}

//LogIf realiza o log da informações no locais pertinentes, no Console e na Queue
//Caso o parâmetro log seja false, nada será feito.
//Caso contrário, será executado a função de log.
//O caso de info, caso não seja nil, não será adicionado mensagens anteriores à mensagem
//Por fim, será executado a action, caso não seja nil
func (l *LoggerSQS) LogIf(log bool, level string, info string, message func() string, action func()) {
	if log {

		json, _ := json.Marshal(&Log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Type: l.ttype, Location: l.location, Level: level, Message: info + "[mensagem:" + message() + "]"})
		//fmt.Println(string(json))
		l.queue.Send(aws.String(string(json)))

		if action != nil {
			action()
		}
	}
}

//NewLogger cria um novo objeto Logger que irá logar numa fila AWS SQS.
func NewLogger(queue Queue, erro error, ttype string, location string) Logger {
	if erro != nil {
		json, _ := json.Marshal(&Log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Type: ttype, Location: location, Level: "FATAL", Message: "[mensagem:" + erro.Error() + "]"})
		fmt.Println(string(json))
		os.Exit(1)
	}
	return &LoggerSQS{queue: queue, ttype: ttype, location: location}
}
