package mydumper

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"testing"
)

func TestIs_disk_space_ok(t *testing.T) {
	o := new(OptionEntries)
	o.global = new(globalEntries)
	o.global.dump_directory = "abcd"
	o.is_disk_space_ok(100)
}

func TestMySQLExecuteSelectStreaming(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "", func(c *client.Conn) {
		return
	})
	if err != nil {
		t.Error(err)
		return
	}
	var result mysql.Result
	var rowsNumber int64
	conn.UseDB("test")
	err = conn.ExecuteSelectStreaming("SELECT /*!40001 SQL_NO_CACHE */ * FROM `migration_state`", &result, func(row []mysql.FieldValue) error {
		rowsNumber++
		return nil
	}, func(result *mysql.Result) error {
		return nil
	})
	t.Log(rowsNumber)
	conn.Close()
}
