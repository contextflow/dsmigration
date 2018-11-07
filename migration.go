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

var (
	// ErrNoNewerVersion error
	ErrNoNewerVersion = errors.New("No newer version")
	// ErrNoOlderVersion error
	ErrNoOlderVersion = errors.New("No older version")
	// ErrNoVersionFound  error
	ErrNoVersionFound = errors.New("No version found")
)

// UpAll applies all migration to the database
func UpAll(db *sql.DB, migs []Migration) error {
	return UpTo(db, migs, 1<<62)
}

// Up one migration
func Up(db *sql.DB, migs []Migration) error {
	state, err := analyze(db, migs)
	if err != nil {
		return err
	}
	return up(db, &state)
}

func up(db *sql.DB, state *migState) error {
	if state.cur.newer == nil {
		return ErrNoNewerVersion
	}

	migration := state.cur.newer.migration

	Logger.Printf("Up migration from version %d to version %d", state.cur.migration.Version, migration.Version)
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
	state.cur = state.cur.newer
	return nil
}

// Down one migration
func Down(db *sql.DB, migs []Migration) error {
	state, err := analyze(db, migs)
	if err != nil {
		return err
	}
	return down(db, &state)
}
func down(db *sql.DB, state *migState) error {
	if state.cur == state.head {
		return ErrNoOlderVersion
	}

	migration := state.cur.migration

	Logger.Printf("Down Migration from version %d to version %d", migration.Version, state.cur.older.migration.Version)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(migration.Down)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "Executing down migration %d", migration.Version)
	}
	_, err = tx.Exec(`
			DELETE FROM migrations WHERE version = ?
		`, migration.Version)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	state.cur = state.cur.older
	return nil
}

// UpTo migrates until version
func UpTo(db *sql.DB, migs []Migration, version int) error {
	state, err := analyze(db, migs)
	if err != nil {
		return err
	}
	for {
		if state.cur.migration.Version >= version {
			break
		}
		err := up(db, &state)
		if err == ErrNoNewerVersion {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Version returns the latest installed migration version
func Version(db *sql.DB) (int, error) {
	rows, err := db.Query(`
			SELECT version FROM migrations ORDER BY version DESC;
		`)
	defer rows.Close()
	if err != nil {
		return 0, err
	}
	if !rows.Next() {
		return 0, ErrNoVersionFound
	}
	var ver int
	err = rows.Scan(&ver)
	return ver, err
}

type migNode struct {
	migration Migration
	older     *migNode
	newer     *migNode
}

type migState struct {
	head *migNode
	cur  *migNode
}

func analyze(db *sql.DB, migs []Migration) (migState, error) {

	sort.Slice(migs, func(i, j int) bool {
		return migs[i].Version < migs[j].Version
	})

	migInfos, err := migrations(db)
	if err != nil {
		return migState{}, err
	}

	head := new(migNode)
	current := head
	node := head

	for i, migration := range migs {
		if i < len(migInfos) {
			// check if versions and hashes match
			migInfo := migInfos[i]
			if migration.Version != migInfo.version {
				return migState{}, fmt.Errorf("Matching migration %d failed, expected migration %d next", migration.Version, migInfo.version)
			}
			if hash(migration) != migInfo.hash {
				return migState{}, fmt.Errorf("Matching hash of migration %d failed, expected hash to bes \"%s\" not \"%s\"next", migration.Version, migInfo.hash, hash(migration))
			}
			next := new(migNode)
			next.older = node
			node.newer = next
			next.migration = migration
			node = next
			current = node
		} else {
			next := new(migNode)
			next.older = node
			node.newer = next
			next.migration = migration
			node = next
		}
	}

	return migState{head, current}, nil
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
