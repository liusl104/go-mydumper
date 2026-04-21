package mydumper

import (
	"testing"

	. "github.com/liusl104/go-mydumper/src"
)

func TestMap(t *testing.T) {
	var data = make(map[string]string)
	data["a"] = "v"
	var v string
	var ok bool
	if v, ok = data["a"]; !ok {
		v = "c"
	}
	t.Log(v)
}

func TestDetermine_show_table_status_columns(t *testing.T) {
	var ecol uint = 0
	var ccol uint = 0
	var collcol uint = 0
	var rowscol uint = 0
	var query string = "SHOW TABLE STATUS"
	Username = "admin"
	Password = "admin123"
	Hostname = "127.0.0.1"
	Port = 3306
	conn := Mysql_conn()
	conn.Conn.Exec("use data")
	var result = M_store_result(conn, query, M_critical, "Error showing tables on: %s - Could not execute query", "data")

	determine_show_table_status_columns(result, &ecol, &ccol, &collcol, &rowscol)
	t.Logf("ecol: %d ccol: %d collcol: %d rowscol: %d", ecol, ccol, collcol, rowscol)
}
