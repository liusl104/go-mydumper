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
