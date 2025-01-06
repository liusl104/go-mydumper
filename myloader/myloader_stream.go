package myloader

import (
	. "go-mydumper/src"
	"sync"
)

var (
	stream_thread *GThreadFunc
)

func initialize_stream(c *configuration) {
	stream_thread = G_thread_new("myloader_stream", new(sync.WaitGroup), 0)
	go process_stream(c)
}
func wait_stream_to_finish() {
	stream_thread.Thread.Wait()
}

func read_stream_line() {

}

func flush() {

}

/*func has_mydumper_suffix(o *OptionEntries, line string) bool {
	return m_filename_has_suffix(o, line, ".dat") ||
		m_filename_has_suffix(o, line, ".sql") ||
		line == "metadata.partial" ||
		strings.HasPrefix(line, "metadata")
}*/

func process_stream(stream_conf *configuration) {
	stream_thread.Thread.Add(1)
	defer stream_thread.Thread.Done()
}
