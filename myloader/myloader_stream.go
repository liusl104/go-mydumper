package myloader

import (
	"strings"
	"sync"
)

func initialize_stream(o *OptionEntries, c *configuration) {
	o.global.stream_thread = new(sync.WaitGroup)
	go process_stream(o, c)
}
func wait_stream_to_finish(o *OptionEntries) {
	o.global.stream_thread.Wait()
}

func read_stream_line() {

}

func flush() {

}

func has_mydumper_suffix(o *OptionEntries, line string) bool {
	return m_filename_has_suffix(o, line, ".dat") ||
		m_filename_has_suffix(o, line, ".sql") ||
		line == "metadata.partial" ||
		strings.HasPrefix(line, "metadata")
}

func process_stream(o *OptionEntries, stream_conf *configuration) {
	o.global.stream_thread.Add(1)
	defer o.global.stream_thread.Done()
}
