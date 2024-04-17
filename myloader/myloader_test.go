package myloader

import (
	"strings"
	"testing"
	"time"
)

func TestPrint_time(t *testing.T) {
	a := time.Now()
	time.Sleep(100 * time.Second)
	t.Log(print_time(a))
}

func TestStartLoad(t *testing.T) {
	var a = "adf.schema.sql.gz"
	var b = "adf.schema.sql"
	s := strings.Repeat(" ", len(a)-len(b))
	s += b
	t.Log(s)
}
