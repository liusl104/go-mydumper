package myloader

import (
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"sync/atomic"
)

type intermediate_filename struct {
	filename   string
	iterations uint
}

func initialize_intermediate_queue(o *OptionEntries, c *configuration) {
	o.global.intermediate_conf = c
	o.global.intermediate_queue = g_async_queue_new(o.Common.BufferSize)
	o.global.exec_process_id = make(map[string]string)
	o.global.exec_process_id_mutex = g_mutex_new()
	o.global.metadata_mutex = g_mutex_new()
	o.global.start_intermediate_thread = g_mutex_new()
	o.global.start_intermediate_thread.Lock()
	if o.Execution.Stream {
		o.global.start_intermediate_thread.Unlock()
	}
	o.global.metadata_mutex.Lock()
	o.global.intermediate_queue_ended = false
	o.global.stream_intermediate_thread = new(sync.WaitGroup)
	go intermediate_thread(o)
	if o.Threads.ExecPerThreadExtension != "" {
		// TODO this is ignore
	}
	initialize_control_job(o, c)
}

func intermediate_queue_new(o *OptionEntries, filename string) {
	var iflnm = new(intermediate_filename)
	iflnm.filename = filename
	iflnm.iterations = 0
	o.global.intermediate_queue.push(iflnm)
}

func intermediate_queue_end(o *OptionEntries) {
	o.global.start_intermediate_thread.Unlock()
	var e = "END"
	intermediate_queue_new(o, e)
	log.Info("Intermediate queue: Sending END job")
	o.global.stream_intermediate_thread.Wait()
	log.Infof("Intermediate thread: SHUTDOWN")
	o.global.intermediate_queue_ended = true
}

func intermediate_queue_incomplete(o *OptionEntries, iflnm *intermediate_filename) {
	iflnm.iterations++
	o.global.intermediate_queue.push(iflnm)
}

func wait_until_first_metadata(o *OptionEntries) {
	o.global.metadata_mutex.Lock()
}

func process_filename(o *OptionEntries, filename string) file_type {
	var ft = get_file_type(o, filename)
	switch ft {
	case INIT:
	case SCHEMA_TABLESPACE:
	case SCHEMA_CREATE:
		atomic.AddUint64(&o.global.schema_counter, 1)
		process_database_filename(o, filename)
		if o.Common.DB != "" {
			ft = DO_NOT_ENQUEUE
			m_remove(o, o.global.directory, filename)
		}
	case SCHEMA_TABLE:
		atomic.AddUint64(&o.global.schema_counter, 1)
		if !process_table_filename(o, filename) {
			return DO_NOT_ENQUEUE
		} else {
			refresh_table_list(o.global.intermediate_conf)
		}
	case SCHEMA_VIEW:
		if !process_schema_view_filename(o, filename) {
			return DO_NOT_ENQUEUE
		}
	case SCHEMA_SEQUENCE:
		if !process_schema_sequence_filename(o, filename) {
			return INCOMPLETE
		}

	case SCHEMA_TRIGGER:
		if !o.Filter.SkipTriggers {
			if !process_schema_filename(o, filename, TRIGGER) {
				return DO_NOT_ENQUEUE
			}
		}
	case SCHEMA_POST:
		// can be enqueued in any order
		if !o.Filter.SkipPost {
			if !process_schema_filename(o, filename, POST) {
				return DO_NOT_ENQUEUE
			}
		}
	case CHECKSUM:
		if !process_checksum_filename(o, filename) {
			return DO_NOT_ENQUEUE
		}
		o.global.intermediate_conf.checksum_list = append(o.global.intermediate_conf.checksum_list, filename)
	case METADATA_GLOBAL:
		if !o.global.first_metadata {
			o.global.metadata_mutex.Unlock()
			o.global.first_metadata = true
		}
		process_metadata_global(o, filename)
		refresh_table_list(o.global.intermediate_conf)
	case DATA:
		if !o.Filter.NoData {
			if !process_data_filename(o, filename) {
				return DO_NOT_ENQUEUE
			}
		} else {
			m_remove(o, o.global.directory, filename)
		}
		o.global.total_data_sql_files++
	case RESUME:
		if o.Execution.Stream {
			log.Fatalf("We don't expect to find resume files in a stream scenario")
		}
	case IGNORED:
		log.Warnf("Filename %s has been ignored", filename)

	case LOAD_DATA:
		release_load_data_as_it_is_close(o, filename)
	case SHUTDOWN:
	case INCOMPLETE:
	default:
		log.Infof("Ignoring file %s", filename)
	}
	return ft
}

func remove_fifo_file(o *OptionEntries, fifo_name string) {
	o.global.exec_process_id_mutex.Lock()
	filename, _ := o.global.exec_process_id[fifo_name]
	o.global.exec_process_id_mutex.Unlock()
	if filename != "" {
		m_remove(o, o.global.directory, filename)
	}

}

func process_stream_filename(o *OptionEntries, iflnm *intermediate_filename) {
	var current_ft = process_filename(o, iflnm.filename)
	if current_ft == INCOMPLETE {
		if iflnm.iterations > 5 {
			log.Warnf("Max renqueing reached for: %s", iflnm.filename)
		} else {
			log.Debugf("Requeuing in intermediate queue %d: %s", iflnm.iterations, iflnm.filename)
			intermediate_queue_incomplete(o, iflnm)
		}
		return
	}
	if current_ft != SCHEMA_VIEW &&
		current_ft != SCHEMA_SEQUENCE &&
		current_ft != SCHEMA_TRIGGER &&
		current_ft != SCHEMA_POST &&
		current_ft != CHECKSUM &&
		current_ft != IGNORED &&
		current_ft != DO_NOT_ENQUEUE {
		refresh_db_and_jobs(o, current_ft)
	}
}

func intermediate_thread(o *OptionEntries) {
	o.global.stream_intermediate_thread.Add(1)
	defer o.global.stream_intermediate_thread.Done()
	var iflnm *intermediate_filename
	o.global.start_intermediate_thread.Lock()
	for {
		iflnm = o.global.intermediate_queue.pop().(*intermediate_filename)
		if strings.Compare(iflnm.filename, "END") == 0 {
			if o.global.intermediate_queue.length > 0 {
				o.global.intermediate_queue.push(iflnm)
				continue
			}
			iflnm = nil
			break
		}
		process_stream_filename(o, iflnm)
		if iflnm == nil {
			break
		}
	}
	var n uint
	for n = 0; n < o.Common.NumThreads; n++ {

	}
	log.Infof("Intermediate thread ended")
	refresh_db_and_jobs(o, INTERMEDIATE_ENDED)
	return
}
