package mydumper

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	"os"
	"sort"
	"sync"
)

var (
	database_hash       map[string]*database
	database_hash_mutex *sync.Mutex
)

type database struct {
	name              string
	filename          string
	escaped           string
	ad_mutex          *sync.Mutex
	already_dumped    bool
	schema_checksum   string
	post_checksum     string
	triggers_checksum string
	dump_triggers     bool
}

// initialize_database initializes database_hash and database_hash_mutex.
func initialize_database() {
	database_hash = make(map[string]*database)
	database_hash_mutex = G_mutex_new()
}

// new_database creates a database struct, fills name/filename/escaped, and inserts it into database_hash.
func new_database(conn *DBConnection, database_name string, already_dumped bool) *database {
	var d *database = new(database)
	_ = conn
	d.name = Backtick_protect(database_name)
	d.filename = get_ref_table(d.name)
	d.escaped = escape_string(d.name)
	d.already_dumped = already_dumped
	d.ad_mutex = G_mutex_new()
	d.schema_checksum = ""
	d.post_checksum = ""
	d.triggers_checksum = ""
	d.dump_triggers = !Is_regex_being_used() && TablesList == "" && len(conf_per_table.All_object_to_export) == 0
	database_hash[d.name] = d
	return d
}

// free_database clears escaped and ad_mutex of the database (no-op for hash removal).
func free_database(d *database) {
	if d.escaped != "" {
		d.escaped = ""
	}
	if d.ad_mutex != nil {
		d.ad_mutex = nil
	}
	d = nil
}

// free_databases clears database_hash and its mutex.
func free_databases() {
	database_hash_mutex.Lock()
	database_hash = nil
	database_hash_mutex.Unlock()
	database_hash_mutex = nil
}

// get_database looks up or creates the database in database_hash; returns true if newly created.
func get_database(conn *DBConnection, database_name string, database **database) bool {
	database_hash_mutex.Lock()
	*database, _ = database_hash[database_name]
	if *database == nil {
		*database = new_database(conn, database_name, false)
		database_hash_mutex.Unlock()
		return true
	}
	database_hash_mutex.Unlock()
	return false
}

// write_database_on_disk writes [database] and checksum lines to the metadata file for each database in database_hash.
func write_database_on_disk(mdfile *os.File) {
	var q = Identifier_quote_character
	var d *database
	var keys []string
	for k, _ := range database_hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, it := range keys {
		d = database_hash[it]
		if d.schema_checksum != "" || d.post_checksum != "" || d.triggers_checksum != "" {
			fmt.Fprintf(mdfile, "\n[%s%s%s]\n", q, d.name, q)
		}
		if d.schema_checksum != "" {
			fmt.Fprintf(mdfile, "%s = %s\n", "schema_checksum", d.schema_checksum)
		}
		if d.post_checksum != "" {
			fmt.Fprintf(mdfile, "%s = %s\n", "post_checksum", d.post_checksum)
		}
		if d.triggers_checksum != "" {
			fmt.Fprintf(mdfile, "%s = %s\n", "triggers_checksum", d.triggers_checksum)
		}
	}
}
