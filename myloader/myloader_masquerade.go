package myloader

import (
	"github.com/go-ini/ini"
	"strconv"
)

func g_key_file_get_value(kf *ini.File, group string, key string) string {
	return kf.Section(group).Key(key).String()
}

func g_ascii_strtoull(s string) int {
	r, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return r
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func intToBool(i int) bool {
	if i == 0 {
		return false
	}
	return true
}
