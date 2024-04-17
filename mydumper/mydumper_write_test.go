package mydumper

import (
	"github.com/go-mysql-org/go-mysql/client"
	"testing"
)

func TestWrite_row_into_file_in_sql_mode(t *testing.T) {
	o := NewDefaultEntries()
	conn, _ := client.Connect("10.23.14.50:5000", "root", "admin123", "test2")
	tj := new(table_job)
	write_row_into_file_in_sql_mode(o, conn, "select * from test2.t2", tj)

}
