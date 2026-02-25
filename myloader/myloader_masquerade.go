package myloader

// boolToInt returns 1 if b is true, 0 otherwise (for replication/SQL options).
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// intToBool returns false if i is 0, true otherwise.
func intToBool(i int) bool {
	if i == 0 {
		return false
	}
	return true
}
