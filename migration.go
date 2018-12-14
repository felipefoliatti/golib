package golib

import (
	"database/sql"
	"fmt"

	"github.com/go-errors/errors"

	migrate "github.com/rubenv/sql-migrate"
)

// Migrator define uma interface para exceutar o migrator
// Este objeto tem apenas uma função, que é rodar as migrations
type Migrator interface {
	Migrate() *errors.Error
}

// SqlMigrateMigrator é o migrator que utiliza o sql-migrate para realizar as migrations
// É a implementação específica do Migrator
type SqlMigrateMigrator struct {
	drivername *string
	url        *string
	database   *string
}

// NewMigrator é o construtor do SqlMigrateMigrator, responsável por iniciar um objeto Migrator
// Para iniciar a instância, é necessário fornecer um nome de banco de dados.
// Caso esse banco informado não exista, então ele será criado
// Caso exista, então apenas uma instância de um Migrator será retornado
func NewMigrator(drivername *string, database *string, url *string) (*SqlMigrateMigrator, *errors.Error) {

	var err *errors.Error

	mig := new(SqlMigrateMigrator)
	mig.url = url
	mig.database = database
	mig.drivername = drivername

	db, e := sql.Open(*mig.drivername, *mig.url)
	err = errors.WrapPrefix(e, "error opening the connection to database", 0)

	if err != nil {
		return nil, err
	}
	_, e = db.Exec("CREATE DATABASE IF NOT EXISTS " + *mig.database)
	err = errors.WrapPrefix(e, "error checking/creating the database", 0)

	db.Close()

	return mig, err
}

// Migrate é responsável por rodar as Migrations num banco de dados já informado para criação do Migrator
// As migrations serão executadas e, caso haja erro, ele será retornado no objeto error
func (m *SqlMigrateMigrator) Migrate() *errors.Error {

	var err *errors.Error

	db, e := sql.Open(*m.drivername, *m.url+*m.database+"?parseTime=true")
	err = errors.WrapPrefix(e, "error opening the connection to database", 0)

	if err != nil {
		return err
	}

	migrations := &migrate.FileMigrationSource{Dir: "./migrations"}
	n, e := migrate.Exec(db, *m.drivername, migrations, migrate.Up)
	err = errors.WrapPrefix(e, "error migrating the database", 0)

	if err != nil {
		return err
	}

	fmt.Printf("Applied %d migrations!\n", n)
	db.Close()
	return nil
}
