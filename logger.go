package golib

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

//Level indica o tipo de error que o logger irá lançar
type Level string

const (
	//INFO é uma log de informação
	INFO Level = "INFO"
	//ERROR é um log de erro
	ERROR = "ERROR"
	//WARN é um log de aviso, quando alguma situação importante acontece
	WARN = "WARN"
	//FATAL é um log de erro grave, que interrompe o funcionamento do programa
	FATAL = "FATAL"
)

type log struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Optional  string `json:"optional,omitempty"`
}

//LoggerImpl define uma implementação de Logger para logar em console
type LoggerImpl struct {
}

//Logger descreve uma interface de log de ero, que loga em diversas fontes, conforme suas implementações
type Logger interface {
	LogIf(canlog bool, level Level, message func() string, action func(*string))
	Log(level Level, message string)
	LogOpt(level Level, message string, optional string)
}

//LogIf realiza o log da informações no locais pertinentes, no Console e na Queue
//Caso verbose seja false, nada será printado no console, caso seja true, os erros também serão mandados para o console
//Caso o parâmetro canlog seja false, nada será feito.
//Caso contrário, será executado a função de log.
//O caso de info, caso não seja nil, não será adicionado mensagens anteriores à mensagem
//Por fim, será executado a action, caso não seja nil
func (l *LoggerImpl) LogIf(canlog bool, level Level, message func() string, action func(*string)) {
	l.log(canlog, level, message, nil, action)
}

func (l *LoggerImpl) log(canlog bool, level Level, message func() string, optional func() string, action func(*string)) {
	if canlog {

		var m string
		var o string

		if message != nil {
			m = message()
		}
		if optional != nil {
			o = optional()
		}

		json, _ := json.Marshal(&log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Level: fmt.Sprint(level), Message: m, Optional: o})
		message := aws.String(string(json))

		if level == ERROR || level == FATAL {
			os.Stderr.WriteString(*message + "\n")
		} else {
			os.Stdout.WriteString(*message + "\n")
		}

		if action != nil {
			action(message)
		}
	}
}

//Log realiza um log simples, informando apenas o level e a mensagem
func (l *LoggerImpl) Log(level Level, message string) {
	l.log(true, level, func() string { return message }, nil, nil)
}

//LogOpt realiza um log simples, informando apenas o level, mensagem e opcional
func (l *LoggerImpl) LogOpt(level Level, message string, optional string) {
	l.log(true, level, func() string { return message }, func() string { return optional }, nil)
}

//NewLogger cria um novo objeto Logger que irá logar no console.
func NewLogger() Logger {
	return &LoggerImpl{}
}
