package golib

import (
	"math"
	"time"
)

//Sleeper define uma interface que avalia um erro, guarda seu estado e, conforme for, pára ou não o processo
type Sleeper interface {
	Eval(err error)
}

//SleeperImpl é a implementação do Sleeper que fornece um cálculo exponencial de paradas quando os erros se repetem
//Conforme o erro se repete, será parado a 2^n segundos, onde n é o númer ode vezes que o erro se repetiu consecutivamente
//Max, por sua vez, define o valor máximo de n
type sleeperImpl struct {
	max    int
	action func(erro error, errors int, wait time.Duration)

	lastError error
	errors    int
}

//Eval realiza uma avaliação do erro para ver se ele é recorrente
//Se ele for, então realiza uma parada, levando em conta o limite max
//Por fim, uma função action será executada, informando se houve ou não parada, e o tempo de parada
func (s *sleeperImpl) Eval(erro error) {
	if s.lastError != nil && erro != nil && s.lastError.Error() == erro.Error() {

		s.errors = int(math.Min(float64(s.errors+1), float64(s.max+1)))       //s.max + 1, já que diminiu um em 2^(s.errors-1)
		wait := time.Second * time.Duration(math.Pow(2, float64(s.errors-1))) //Começa a esperar após o segundo erro igual, logo s.erros-1 para começar a base de espera em 1

		s.action(erro, s.errors, wait)

		time.Sleep(wait) //espera no máximo 5min (2⁸s) se max = 8

	} else if erro != nil {

		s.lastError = erro
		s.errors = 1
		s.action(erro, s.errors, 0)

	} else {

		s.action(nil, 0, 0)
		s.lastError = nil
		s.errors = 0
	}

}

//NewSleeper cria um Sleeper padrão
func NewSleeper(max int, action func(erro error, errors int, wait time.Duration)) Sleeper {
	return &sleeperImpl{max: max, action: action, lastError: nil, errors: 0}
}
