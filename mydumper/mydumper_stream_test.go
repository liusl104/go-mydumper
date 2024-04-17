package mydumper

import (
	"bufio"
	"io"
	"os"
	"testing"
)

func TestProcess_stream(t *testing.T) {
	f, err := os.Open("/Users/liu/GolandProjects/go-mydumper/outout/metadata")
	if err != nil {
		t.Fail()
	}
	var buf []byte = make([]byte, 100)
	var buflen, length int
	reader := bufio.NewReader(f)
	buflen, err = reader.Read(buf)
	if buflen < 100 {
		buf = buf[:buflen]
	}
	defer f.Close()
	for buflen > 0 {
		length, err = os.Stdout.Write(buf)
		if buflen != length {
			t.Fatalf("fail")
		}
		buf = make([]byte, 100)
		buflen, err = reader.Read(buf)
		if err == io.EOF {
			return
		}
		if buflen < 100 {
			buf = buf[:buflen]
		}

	}

}
