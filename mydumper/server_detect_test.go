package mydumper

import (
	"testing"
)

func TestDetect_server_version(t *testing.T) {
	o := newEntries()
	err := detect_server_version(o, nil)
	if err != nil {
		t.Fatal(err)
	} else {
		t.Log("ok")
	}
}
