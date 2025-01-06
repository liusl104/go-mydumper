package myloader

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
