package golib

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

//Statement representa uma estrutura de instrução ao banco de dados
//Ela é composta pelo statement, que é uma string parametrizada com o comando e pelos argumentos que irão substituir esses padrões
type Statement struct {
	statement string
	args      []interface{}
}

// Database define uma interface de comunicação com o banco de dados
// Através desta interface, será possível executar instruções no banco de dados
// Em caso de erro, um objeto error é retornado
type Database interface {
	Run(statement ...Statement) error
}

// MySqlDatabase é uma implementação concreta da interface Database para MySql
// O nome do banco de dados é utilizado para conectar
// A Url é o endereço para o banco de dados
type mySqlDatabase struct {
	drivername *string
	url        *string
	database   *string
}

// NewDatabase cria uma instância concreta do MySqlDatabase
// Para criar um banco de dados é necessário informar o nome do banco de dados, bem como a url para conectar a ele
// Caso exista, então apenas uma instância de um Migrator será retornado
func NewDatabase(drivername *string, database *string, url *string) Database {

	my := new(mySqlDatabase)
	my.drivername = drivername
	my.url = url
	my.database = database

	return my
}

// Run realiza a execução de uma instrução SQL
// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Run(statements ...Statement) error {

	db, err := sql.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/)
	if err != nil {
		return err
	}

	tx, err := db.Begin()

	for _, statement := range statements {
		//interpolateParams=true
		_, err = db.Exec(statement.statement, statement.args...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	err = tx.Commit()

	defer db.Close()
	return err
}
