package mydumper

import (
	"fmt"
	. "go-mydumper/src"
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

func free_database(d *database) {
	if d.escaped != "" {
		d.escaped = ""
	}
	if d.ad_mutex != nil {
		d.ad_mutex = nil
	}
	d = nil
}

func initialize_database() {
	database_hash = make(map[string]*database)
	database_hash_mutex = G_mutex_new()
}

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

func free_databases() {
	database_hash_mutex.Lock()
	database_hash = nil
	database_hash_mutex.Unlock()
	database_hash_mutex = nil
}

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
