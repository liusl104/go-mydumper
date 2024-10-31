package mydumper

import (
	"github.com/go-mysql-org/go-mysql/client"
	"testing"
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

func TestIsView(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "test")
	if err != nil {
		t.Fatalf("%v", err)
	}
	o := newEntries()
	o.global.detected_server = SERVER_TYPE_MYSQL
	err = conn.Ping()
	result, err := conn.Execute("SHOW TABLE STATUS FROM kae LIKE 'kae_config'")
	var ecol int = -1
	var ccol int = -1
	var collcol int = -1
	var rowscol int = 0
	col := result.Fields

	determine_show_table_status_columns(result.Fields, &ecol, &ccol, &collcol, &rowscol)
	t.Log(string(col[ccol].Name))
	for _, row := range result.Values {
		var is_view bool
		v1 := row[ccol].Value()
		v2 := row[ccol].AsString()
		t.Log(v1)
		if row[ccol].Value() == nil {
			t.Log("1")
		}
		t.Log(v2)

		if (o.global.detected_server == SERVER_TYPE_MYSQL || o.global.detected_server == SERVER_TYPE_MARIADB) && (row[ccol].Value() == nil || string(row[ccol].AsString()) == "VIEW") {
			is_view = true
		}
		if is_view {
			t.Fail()
		} else {
			t.Log("ok")
		}
	}
}
