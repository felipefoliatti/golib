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

//Data indica um tipo de struct que possui campos opcionais
type Data struct {
	Message  string    `json:"message,omitempty"`
	Location []float64 `json:"location,omitempty"`
	Optional string    `json:"optional,omitempty"`
}

type log struct {
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	Data
}

//LoggerImpl define uma implementação de Logger para logar em console
type LoggerImpl struct {
}

//Logger descreve uma interface de log de ero, que loga em diversas fontes, conforme suas implementações
type Logger interface {
	LogIf(canlog bool, level Level, data func() Data, action func(*string))
	Log(level Level, data Data)
}

//LogIf realiza o log da informações no locais pertinentes, no Console e na Queue
//Caso verbose seja false, nada será printado no console, caso seja true, os erros também serão mandados para o console
//Caso o parâmetro canlog seja false, nada será feito.
//Caso contrário, será executado a função de log.
//O caso de info, caso não seja nil, não será adicionado mensagens anteriores à mensagem
//Por fim, será executado a action, caso não seja nil
func (l *LoggerImpl) LogIf(canlog bool, level Level, data func() Data, action func(*string)) {
	l.log(canlog, level, data, action)
}

func (l *LoggerImpl) log(canlog bool, level Level, data func() Data, action func(*string)) {
	if canlog {

		var d Data

		if data != nil {
			d = data()
		} else {
			d = Data{}
		}

		json, _ := json.Marshal(&log{Timestamp: time.Now().Format("2006-01-02T15:04:05.000000Z"), Level: fmt.Sprint(level), Data: d})
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
func (l *LoggerImpl) Log(level Level, data Data) {
	l.log(true, level, func() Data { return data }, nil)
}

//LogOpt realiza um log simples, informando apenas o level, mensagem e opcional
func (l *LoggerImpl) LogOpt(level Level, data Data) {
	l.log(true, level, func() Data { return data }, nil)
}

//NewLogger cria um novo objeto Logger que irá logar no console.
func NewLogger() Logger {
	return &LoggerImpl{}
}
