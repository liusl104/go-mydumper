package mydumper

func initialize_masquerade(o *OptionEntries) {
	o.global.file_hash = make(map[string]map[string][]string)
}

func finalize_masquerade(o *OptionEntries) {
	o.global.file_hash = nil
}
