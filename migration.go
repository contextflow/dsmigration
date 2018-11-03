package dsmigrate

import (
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"

	"github.com/pkg/errors"
)

// Migration defines a database migration
type Migration struct {
	Version int
	Up      string
	Down    string
}

// UpAll applies all migration to the database
func UpAll(db *sql.DB, migs []Migration) error {
	return UpTo(db, migs, 1<<31)
}

func UpTo(db *sql.DB, migs []Migration, version int) error {

	sort.Slice(migs, func(i, j int) bool {
		return migs[i].Version < migs[j].Version
	})

	migInfos, err := migrations(db)
	if err != nil {
		return err
	}

	for i, migration := range migs {
		if migration.Version > version {
			return nil
		}
		if i < len(migInfos) {
			// check if versions and hashes match
			migInfo := migInfos[i]
			if migration.Version != migInfo.version {
				return fmt.Errorf("Matching migration %d failed, expected migration %d next", migration.Version, migInfo.version)
			}
			if hash(migration) != migInfo.hash {
				return fmt.Errorf("Matching hash of migration %d failed, expected hash to bes \"%s\" not \"%s\"next", migration.Version, migInfo.hash, hash(migration))
			}
		} else {
			h := hash(migration)
			_ = h
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			_, err = tx.Exec(migration.Up)
			if err != nil {
				tx.Rollback()
				return errors.Wrapf(err, "Executing up migration %d", migration.Version)
			}
			_, err = tx.Exec(`
				INSERT INTO migrations (version, hash)
					VALUES (?,?)
				`, migration.Version, h)
			if err != nil {
				tx.Rollback()
				return err
			}
			tx.Commit()
		}
	}

	return nil
}

type migrationInfo struct {
	version int
	hash    string
}

type migrationTable []migrationInfo

func migrations(db *sql.DB) (migrationTable, error) {
	rows, err := db.Query(`
		SELECT version, hash FROM migrations ORDER BY version;
	`)

	if err != nil {
		// Maybe table does not exist, create it
		_, err := db.Exec(`
			CREATE TABLE migrations (
				version INT,
				hash TEXT
			);
		`)
		if err != nil {
			return nil, errors.Wrap(err, "Cannot create miogrations table")
		}

		rows, err = db.Query(`
			SELECT version, hash FROM migrations ORDER BY version;
		`)
	}

	var result migrationTable
	for rows.Next() {
		var mi migrationInfo
		rows.Scan(&mi.version, &mi.hash)
		result = append(result, mi)
	}

	return result, nil

}

func hash(mig Migration) string {
	digest := sha256.New()
	digest.Write([]byte(mig.Up))
	digest.Write([]byte(mig.Down))
	digest.Write([]byte(strconv.Itoa(mig.Version)))
	hash := digest.Sum(nil)

	return base64.StdEncoding.EncodeToString(hash)
}
