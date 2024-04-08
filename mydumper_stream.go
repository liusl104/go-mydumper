package mydumper

func stream_queue_push(o *OptionEntries, dbt *db_table, filename string) {

}

func send_initial_metadata(o *OptionEntries) {

}

func initialize_stream() {

}

func wait_stream_to_finish(o *OptionEntries) {
	o.global.stream_thread.Wait()
}
