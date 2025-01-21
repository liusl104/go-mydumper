package mydumper

import (
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
