package mydumper

import (
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"testing"
)

/*
func TestMysqlQuery(t *testing.T) {
	conf := mysql.Config{Addr: "10.23.14.50:5000", User: "root", Passwd: "admin123"}

	conn, err := sql.Open("mysql", conf.FormatDSN())
	if err != nil {
		t.Fail()
	}
	result, err := mysqlQuery(conn, "show processlist")
	for _, row := range result.result {
		fmt.Printf("%s\n", *row[0].(*[]byte))
	}
	if err != nil {
		t.Fail()
	}
}
*/

func TestGomysql(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "test2")
	if err != nil {
		t.Fatalf("%v", err)
	}
	conn.Ping()
	var result mysql.Result
	err = conn.ExecuteSelectStreaming("select * from a1", &result, func(row []mysql.FieldValue) error {
		for idx, _ := range row {
			t.Log(idx)
		}
		return nil
	}, func(result *mysql.Result) error {
		t.Log("ok")
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("status: %d", result.Status)

	defer conn.Close()
}

func TestConfigure_connection(t *testing.T) {
	o := newEntries()
	o.Connection.Username = "root"
	o.Connection.Password = "admin123"
	o.Connection.Hostname = "10.23.14.50"
	o.Connection.Port = 5000
	o.Connection.Protocol = "tcp"
	c, err := configure_connection(o)
	if err != nil {
		t.Fail()
	}
	err = c.Ping()
	if err != nil {
		t.Fail()
	}
	// 	res, err := c.Execute("")
	// res.Values
	print_connection_details_once(o)
}

func TestErrorCode(t *testing.T) {
	conn, err := client.Connect("10.23.14.50:5000", "root", "admin123", "test")
	if err != nil {
		t.Fail()
	}
	res, err := conn.Execute("select * from abd")
	if err != nil {
		t.Logf("%d", mysqlError(err).Code)
		return
	}
	t.Logf("%d", res.Status)
}
