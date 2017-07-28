package golib

import (
	"encoding/json"
	"log"
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
	LogIf(canlog bool, verbose bool, level string, info string, message func() string, action func(*string))
}

//LoggerSQS define uma implementação de Logger para logar em filas SQS da Amazon, além do console
type LoggerSQS struct {
	queue    Queue
	ttype    string
	location string
}

//LogIf realiza o log da informações no locais pertinentes, no Console e na Queue
//Caso verbose seja false, nada será printado no console, caso seja true, os erros também serão mandados para o console
//Caso o parâmetro canlog seja false, nada será feito.
//Caso contrário, será executado a função de log.
//O caso de info, caso não seja nil, não será adicionado mensagens anteriores à mensagem
//Por fim, será executado a action, caso não seja nil
func (l *LoggerSQS) LogIf(canlog bool, verbose bool, level string, info string, message func() string, action func(*string)) {
	if canlog {

		json, _ := json.Marshal(&Log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Type: l.ttype, Location: l.location, Level: level, Message: info + "[mensagem:" + message() + "]"})
		message := aws.String(string(json))
		//fmt.Println(string(json))
		l.queue.Send(message)

		if verbose == true {
			log.Println(*message)
		}

		if action != nil {
			action(message)
		}
	}
}

//NewLogger cria um novo objeto Logger que irá logar numa fila AWS SQS.
func NewLogger(queue Queue, erro error, ttype string, location string) Logger {
	if erro != nil {
		json, _ := json.Marshal(&Log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Type: ttype, Location: location, Level: "FATAL", Message: "[mensagem:" + erro.Error() + "]"})
		log.Print(string(json))
		os.Exit(1)
	}
	return &LoggerSQS{queue: queue, ttype: ttype, location: location}
}
