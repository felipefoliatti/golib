package golib

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//Statement representa uma estrutura de instrução ao banco de dados
//Ela é composta pelo statement, que é uma string parametrizada com o comando e pelos argumentos que irão substituir esses padrões
type Statement struct {
	Statement string
	Args      []interface{}
}

// Database define uma interface de comunicação com o banco de dados
// Através desta interface, será possível executar instruções no banco de dados
// Em caso de erro, um objeto error é retornado
type Database interface {
	Run(statement ...Statement) ([]sql.Result, error)
	Query(dest interface{}, statement Statement) error
	Transaction(fun func(db *sqlx.DB) error) error
	Do(act func(db *sqlx.DB)) error
}

// MySqlDatabase é uma implementação concreta da interface Database para MySql
// O nome do banco de dados é utilizado para conectar
// A Url é o endereço para o banco de dados
type mySqlDatabase struct {
	drivername *string
	url        *string
	database   *string
	timezone   string
	db         *sqlx.DB
}

// NewDatabase cria uma instância concreta do MySqlDatabase
// Para criar um banco de dados é necessário informar o nome do banco de dados, bem como a url para conectar a ele
// Caso exista, então apenas uma instância de um Migrator será retornado
func NewDatabase(drivername *string, database *string, url *string) Database {

	my := new(mySqlDatabase)
	my.drivername = drivername
	my.url = url
	my.database = database
	my.timezone = "UTC"
	return my
}

func (m *mySqlDatabase) Transaction(fun func(db *sqlx.DB) error) error {
	var err error

	if m.db == nil {
		m.db, err = sqlx.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/)
		m.db.SetMaxOpenConns(5)
	}

	if err != nil {
		return err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	err = fun(m.db)

	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	return err
}

// Run realiza a execução de uma instrução SQL
// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Run(statements ...Statement) ([]sql.Result, error) {

	var err error
	results := []sql.Result{}

	if m.db == nil {
		m.db, err = sqlx.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/)
		m.db.SetMaxOpenConns(5)
	}

	if err != nil {
		return nil, err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return nil, err
	}

	for _, statement := range statements {
		//interpolateParams=true
		var result sql.Result
		result, err = tx.Exec(statement.Statement, statement.Args...)

		if err != nil {
			tx.Rollback()
			return nil, err
		}

		results = append(results, result)

	}

	err = tx.Commit()

	//defer db.Close()
	return results, err
}

// Query realiza a execução de uma consulta SQL
// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Query(dest interface{}, statements Statement) error {

	var err error
	if m.db == nil {
		m.db, err = sqlx.Open(*m.drivername, *m.url+*m.database+"?parseTime=true" /*+"?interpolateParams=true"*/)
		m.db.SetMaxOpenConns(5)
	}

	if err != nil {
		return err
	}

	err = m.db.Select(dest, statements.Statement, statements.Args...)

	//defer db.Close()
	return err
}

// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Do(act func(db *sqlx.DB)) error {

	var err error
	if m.db == nil {
		m.db, err = sqlx.Open(*m.drivername, *m.url+*m.database+"?parseTime=true" /*+"?interpolateParams=true"*/)
		m.db.SetMaxOpenConns(5)
	}

	if err != nil {
		return err
	}

	act(m.db)
	return nil
}
