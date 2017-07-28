package golib

import (
	"database/sql"
	"fmt"

	migrate "github.com/rubenv/sql-migrate"
)

// Migrator define uma interface para exceutar o migrator
// Este objeto tem apenas uma função, que é rodar as migrations
type Migrator interface {
	Migrate() error
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
func NewMigrator(drivername *string, database *string, url *string) (*SqlMigrateMigrator, error) {

	mig := new(SqlMigrateMigrator)
	mig.url = url
	mig.database = database
	mig.drivername = drivername

	db, err := sql.Open(*mig.drivername, *mig.url)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS " + *mig.database)
	db.Close()

	return mig, err
}

// Migrate é responsável por rodar as Migrations num banco de dados já informado para criação do Migrator
// As migrations serão executadas e, caso haja erro, ele será retornado no objeto error
func (m *SqlMigrateMigrator) Migrate() error {

	db, err := sql.Open(*m.drivername, *m.url+*m.database+"?parseTime=true")
	migrations := &migrate.FileMigrationSource{Dir: "./migrations"}
	n, err := migrate.Exec(db, *m.drivername, migrations, migrate.Up)

	if err != nil {
		return err
	}
	fmt.Printf("Applied %d migrations!\n", n)
	db.Close()
	return nil
}
