package golib

import (
	"database/sql"
	"strings"

	"github.com/felipefoliatti/errors"

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
	Run(statement ...Statement) ([]sql.Result, *errors.Error)
	RunMisc(statements ...Statement) ([]interface{}, *errors.Error)
	Query(dest interface{}, statement Statement) *errors.Error
	Transaction(fun func(tx *sqlx.Tx) *errors.Error) *errors.Error
	Do(act func(db *sqlx.DB) *errors.Error) *errors.Error
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

func (m *mySqlDatabase) Transaction(fun func(tx *sqlx.Tx) *errors.Error) *errors.Error {

	var e error
	var err *errors.Error

	if m.db == nil {
		m.db, e = sqlx.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/ +"?parseTime=true")
		err = errors.WrapInner("error opening the database", e, 0)
		if err == nil {
			m.db.SetMaxOpenConns(5)
		}
	}

	if err != nil {
		return err
	}

	tx, e := m.db.Beginx()
	err = errors.WrapInner("error beginning the transaction", e, 0)

	if err != nil {
		return err
	}

	err = fun(tx)

	if err != nil {
		tx.Rollback()
		return err
	}

	e = tx.Commit()
	err = errors.WrapInner("error commiting the transaction", e, 0)

	return err
}

// Run statements in a given transaction
// if runs succesful, the first return will have an array of results and the error return will be nil
// if the run fail, the first return will be nil and the error return will have the error
func (m *mySqlDatabase) RunTx(tx *sqlx.Tx, statements ...Statement) ([]sql.Result, *errors.Error) {
	//Run the instructions
	var res []sql.Result
	var err *errors.Error

	for _, statement := range statements {
		var r sql.Result
		var e error
		r, e = tx.Exec(statement.Statement, statement.Args...)
		err = errors.WrapInner("error executing the update", e, 0)
		res = append(res, r)

		if err != nil {
			return nil, err
		}
	}

	return res, err
}

// Run realiza a execução de uma instrução SQL
// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Run(statements ...Statement) ([]sql.Result, *errors.Error) {

	var e error
	var err *errors.Error

	results := []sql.Result{}

	if m.db == nil {
		m.db, e = sqlx.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/)
		err = errors.WrapInner("error opening the database", e, 0)
		if err == nil {
			m.db.SetMaxOpenConns(5)
		}
	}

	if err != nil {
		return nil, err
	}

	tx, e := m.db.Begin()
	err = errors.WrapInner("error beginning the transaction", e, 0)

	if err != nil {
		return nil, err
	}

	for _, statement := range statements {
		//interpolateParams=true
		var result sql.Result
		result, e = tx.Exec(statement.Statement, statement.Args...)
		err = errors.WrapInner("error executing the statement", e, 0)

		if err != nil {
			tx.Rollback()
			return nil, err
		}

		results = append(results, result)

	}

	e = tx.Commit()
	err = errors.WrapInner("error commiting the transaction", e, 0)

	//defer db.Close()
	return results, err
}

// Runs commands or queries inside a transaction. If the Statement contains a SELECT, will return sql.Rows, otherwise the object will be sql.Result
// If any error, the object is returned
func (m *mySqlDatabase) RunMisc(statements ...Statement) ([]interface{}, *errors.Error) {

	var e error
	var err *errors.Error
	results := []interface{}{}

	if m.db == nil {
		m.db, e = sqlx.Open(*m.drivername, *m.url+*m.database /*+"?interpolateParams=true"*/)
		err = errors.WrapInner("error opening the database", e, 0)
		if err == nil {
			m.db.SetMaxOpenConns(5)
		}
	}

	if err != nil {
		return nil, err
	}

	tx, e := m.db.Begin()
	err = errors.WrapInner("error beginning the transaction", e, 0)

	if err != nil {
		return nil, err
	}

	for _, statement := range statements {

		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(statement.Statement)), "SELECT") {
			var result *sql.Rows
			result, e = tx.Query(statement.Statement, statement.Args...)
			err = errors.WrapInner("error executing the query", e, 0)

			if err != nil {
				tx.Rollback()
				return nil, err
			}

			results = append(results, result)
		} else {
			//interpolateParams=true
			var result sql.Result
			result, e = tx.Exec(statement.Statement, statement.Args...)
			err = errors.WrapInner("error executing the statement", e, 0)

			if err != nil {
				tx.Rollback()
				return nil, err
			}

			results = append(results, result)
		}
	}

	e = tx.Commit()
	err = errors.WrapInner("error commiting the transaction", e, 0)

	//defer db.Close()
	return results, err
}

// Query realiza a execução de uma consulta SQL
// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Query(dest interface{}, statements Statement) *errors.Error {

	var e error
	var err *errors.Error

	if m.db == nil {
		m.db, e = sqlx.Open(*m.drivername, *m.url+*m.database+"?parseTime=true" /*+"?interpolateParams=true"*/)
		err = errors.WrapInner("error opening the database", e, 0)
		if err == nil {
			m.db.SetMaxOpenConns(5)
		}
	}

	if err != nil {
		return err
	}

	e = m.db.Select(dest, statements.Statement, statements.Args...)
	err = errors.WrapInner("error executing the select", e, 0)

	//defer db.Close()
	return err
}

// Se houver um erro, um objeto error é retornado
func (m *mySqlDatabase) Do(act func(db *sqlx.DB) *errors.Error) *errors.Error {

	var e error
	var err *errors.Error

	if m.db == nil {
		m.db, e = sqlx.Open(*m.drivername, *m.url+*m.database+"?parseTime=true" /*+"?interpolateParams=true"*/)
		err = errors.WrapInner("error opening the database", e, 0)
		if err == nil {
			m.db.SetMaxOpenConns(5)
		}
	}

	if err != nil {
		return err
	}

	return act(m.db)
}
