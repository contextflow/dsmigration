package main

import (
	"database/sql"
	"log"

	"github.com/flicaflow/dsmigrate"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", "file:locked.sqlite?cache=shared")
	if err != nil {
		log.Fatal("Could not open database", err)
	}

	err = dsmigrate.UpAll(db, migrations)
	if err != nil {
		log.Fatal("1", err)
	}
	err = dsmigrate.Down(db, migrations)
	if err != nil {
		log.Fatal("2", err)
	}
	err = dsmigrate.Down(db, migrations)
	if err != nil {
		log.Fatal("3", err)
	}
	err = dsmigrate.UpAll(db, migrations)
	if err != nil {
		log.Fatal("5", err)
	}
}

var migrations = []dsmigrate.Migration{
	dsmigrate.Migration{
		Version: 1,
		Up: `
		CREATE TABLE hello (
			id INT,
			text STRING);
		`,
		Down: `
		DELETE TABLE hello;
		`,
	},
	dsmigrate.Migration{
		Version: 3,
		Up: `
		CREATE TABLE hello2 (
			id INT,
			text STRING);
		`,
		Down: `
		DROP TABLE hello2;
		`,
	},
	dsmigrate.Migration{
		Version: 4,
		Up: `
		INSERT INTO hello2
			(id, text)
		VALUES 
			(1, 'cjwjbcfkjew'),
			(2, 'xqmkqccw');
		`,
		Down: `
		DELETE FROM hello2
			WHERE id = 1 OR id = 2; 
		`,
	},
}
