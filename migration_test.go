package dsmigration

import (
	"database/sql"
	"log"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

var testMigrations = []Migration{
	Migration{
		Version: 1,
		Up:      `CREATE TABLE hello (id INT, text STRING);`,
		Down:    `DROP TABLE hello;`,
	},
	Migration{
		Version: 3,
		Up:      `CREATE TABLE hello2 (id INT,text STRING);`,
		Down:    `DROP TABLE hello2;`,
	},
	Migration{
		Version: 4,
		Up: `INSERT INTO hello2	(id, text) VALUES (1, 'cjwjbcfkjew'),(2, 'xqmkqccw');`,
		Down: `DELETE FROM hello2 WHERE id = 1 OR id = 2;`,
	},
}

func TestUpAll(t *testing.T) {
	db, err := sql.Open("sqlite3", "file:locked.sqlite?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}

	err = UpAll(db, testMigrations)
	if err != nil {
		t.Fatal(err)
	}

	ver, err := Version(db)
	if ver != 4 {
		t.Fatalf("Expected version %d got %d", 4, ver)
	}
}

func TestUpDownDance(t *testing.T) {
	Logger = log.New(os.Stderr, "", log.LstdFlags)
	db, err := sql.Open("sqlite3", "file:updowndance.sqlite?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}

	err = Up(db, testMigrations)
	if err != nil {
		t.Fatal(err)
	}
	err = Down(db, testMigrations)
	if err != nil {
		t.Fatal(err)
	}

	for {
		err = Up(db, testMigrations)
		if err == ErrNoNewerVersion {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		err = Up(db, testMigrations)
		if err == ErrNoNewerVersion {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		err = Down(db, testMigrations)
		if err != nil {
			t.Fatal(err)
		}

	}

}
