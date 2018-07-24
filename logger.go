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

	LogAIf(canlog bool, level Level, data func() interface{}, action func(*string))
	LogA(level Level, data interface{})
	LogAIf2(canlog bool, level Level, data interface{})
}

func (l *LoggerImpl) LogIf(canlog bool, level Level, data func() Data, action func(*string)) {
	l.log(canlog, level, func() interface{} { return data() }, action)
}

func (l *LoggerImpl) Log(level Level, data Data) {
	l.log(true, level, func() interface{} { return data }, nil)
}

func (l *LoggerImpl) LogAIf(canlog bool, level Level, data func() interface{}, action func(*string)) {
	l.log(canlog, level, data, action)
}

func (l *LoggerImpl) LogA(level Level, data interface{}) {
	l.log(true, level, func() interface{} { return data }, nil)
}

func (l *LoggerImpl) LogAIf2(canlog bool, level Level, data interface{}) {
	l.log(canlog, level, func() interface{} { return data }, nil)
}

func (l *LoggerImpl) log(canlog bool, level Level, data func() interface{}, action func(*string)) {
	if canlog {

		m := make(map[string]interface{})

		if data != nil {
			d := data()
			record, _ := json.Marshal(d)
			json.Unmarshal(record, &m)
		}

		m["timestamp"] = time.Now().Format("2006-01-02T15:04:05.000000Z")
		m["level"] = fmt.Sprint(level)

		json, _ := json.Marshal(m)
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

//NewLogger cria um novo objeto Logger que irá logar no console.
func NewLogger() Logger {
	return &LoggerImpl{}
}
