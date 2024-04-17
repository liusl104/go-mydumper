package mydumper

import "testing"

func Test_New_real_partition_step(t *testing.T) {
	new_real_partition_step([]string{"a"}, 1, 1)
}

func TestGetEscapedMiddleChar(t *testing.T) {
	// getEscapedMiddleChar(nil, []byte("U0b8pV"), []byte("U0b8pV"), 1)
	data := get_escaped_middle_char(nil, []byte("ZZZRd8"), 1, []byte("P03gdG"), 6, 2)
	if data == "U" {
		t.Log("OK")
	} else {
		t.Fail()
	}
}
