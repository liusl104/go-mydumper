package mydumper

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/client"
	"os"
	"sort"
	"sync"
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

func initialize_database(o *OptionEntries) {
	o.global.database_hash = make(map[string]*database)
	o.global.database_hash_mutex = g_mutex_new()
}

func new_database(o *OptionEntries, conn *client.Conn, database_name string, already_dumped bool) *database {
	var d *database = new(database)
	_ = conn
	d.name = backtick_protect(database_name)
	d.filename = get_ref_table(o, d.name)
	d.escaped = escape_string(d.name)
	d.already_dumped = already_dumped
	d.ad_mutex = g_mutex_new()
	d.schema_checksum = ""
	d.post_checksum = ""
	d.triggers_checksum = ""
	d.dump_triggers = !is_regex_being_used(o) && o.CommonFilter.TablesList == "" && len(o.global.conf_per_table.all_object_to_export) == 0
	o.global.database_hash[d.name] = d
	return d
}

func free_databases(o *OptionEntries) {
	o.global.database_hash_mutex.Lock()
	o.global.database_hash = nil
	o.global.database_hash_mutex.Unlock()
	o.global.database_hash_mutex = nil
}

func get_database(o *OptionEntries, conn *client.Conn, database_name string, database **database) bool {
	o.global.database_hash_mutex.Lock()
	*database, _ = o.global.database_hash[database_name]
	if *database == nil {
		*database = new_database(o, conn, database_name, false)
		o.global.database_hash_mutex.Unlock()
		return true
	}
	o.global.database_hash_mutex.Unlock()
	return false
}

func write_database_on_disk(o *OptionEntries, mdfile *os.File) {
	var q = o.Common.IdentifierQuoteCharacter
	var d *database
	var keys []string
	for k, _ := range o.global.database_hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, it := range keys {
		d = o.global.database_hash[it]
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
