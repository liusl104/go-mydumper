package mydumper

import (
	"os"
	"testing"
)

func openFIle(file *os.File) {
	file, _ = os.Open("create.sh")
	return
}

func TestOSFile(t *testing.T) {
	var file *os.File
	openFIle(file)
	if file == nil {
		t.Fail()
	} else {
		t.Log("ok")
	}
}
func appQueue(c *configuration) {
	c.non_innodb.queue.push(1)
}

func TestConfiguration(t *testing.T) {
	c := new(configuration)
	c.non_innodb = new(table_queuing)
	c.non_innodb.queue = g_async_queue_new(10)
	t.Logf("query length: %d", c.non_innodb.queue.length)
	appQueue(c)
	t.Logf("query length: %d", c.non_innodb.queue.length)
}
