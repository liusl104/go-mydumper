package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	"sync"
)

var (
	stream_thread         *GThread
	metadata_header_mutex *sync.Mutex
	metadata_header_cond  *sync.Cond
)

func initialize_stream(c *configuration) {
	stream_thread = M_thread_new("myloader_stream", process_stream, c, "Stream thread could not be created")
	metadata_header_mutex = G_mutex_new()
	metadata_header_cond = &sync.Cond{}
}
func wait_stream_to_finish() {
	G_thread_join(stream_thread)
}

func wait_stream_to_process_metadata_header() {

}

func metadata_has_been_processed() {

}
func read_stream_line() {

}

func flush() {

}

func has_mydumper_suffix(line string) bool {
	return false
}

/*func has_mydumper_suffix(o *OptionEntries, line string) bool {
	return m_filename_has_suffix(o, line, ".dat") ||
		m_filename_has_suffix(o, line, ".sql") ||
		line == "metadata.partial" ||
		strings.HasPrefix(line, "metadata")
}*/

func process_stream(c any) {
	stream_conf := c.(*configuration)
	_ = stream_conf
}
