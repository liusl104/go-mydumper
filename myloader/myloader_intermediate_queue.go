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

func (o *OptionEntries) initialize_intermediate_queue(c *configuration) {
	o.global.intermediate_conf = c
	o.global.intermediate_queue = g_async_queue_new(o.BufferSize)
	o.global.exec_process_id = make(map[string]string)
	o.global.exec_process_id_mutex = g_mutex_new()
	o.global.start_intermediate_thread = g_mutex_new()
	o.global.start_intermediate_thread.Lock()
	if o.Stream {
		o.global.start_intermediate_thread.Unlock()
	}
	o.global.intermediate_queue_ended = false
	o.global.stream_intermediate_thread = new(sync.WaitGroup)
	go o.intermediate_thread()
	if o.ExecPerThreadExtension != "" {
		// TODO this is ignore
		if o.ExecPerThread == "" {
			log.Errorf("--exec-per-thread needs to be set when --exec-per-thread-extension is used")
		}
	}
	if o.ExecPerThread != "" {
		if o.ExecPerThread[0] != '/' {
			log.Errorf("Absolute path is only allowed when --exec-per-thread is used")
		}
		o.global.exec_per_thread_cmd = strings.Split(o.ExecPerThread, " ")
	}
	o.initialize_control_job(c)
}

func (o *OptionEntries) intermediate_queue_new(filename string) {
	var iflnm = new(intermediate_filename)
	iflnm.filename = filename
	iflnm.iterations = 0
	log.Debugf("intermediate_queue <- %s (%d)", iflnm.filename, iflnm.iterations)
	o.global.intermediate_queue.push(iflnm)
}

func (o *OptionEntries) intermediate_queue_end() {
	o.global.start_intermediate_thread.Unlock()
	var e = "END"
	o.intermediate_queue_new(e)
	log.Info("Intermediate queue: Sending END job")
	o.global.stream_intermediate_thread.Wait()
	log.Infof("Intermediate thread: SHUTDOWN")
	o.global.intermediate_queue_ended = true
}

func (o *OptionEntries) intermediate_queue_incomplete(iflnm *intermediate_filename) {
	iflnm.iterations++
	log.Debugf("intermediate_queue <- %s (%d) incomplete", iflnm.filename, iflnm.iterations)
	o.global.intermediate_queue.push(iflnm)
}

func (o *OptionEntries) process_filename(filename string) file_type {
	var ft = o.get_file_type(filename)
	switch ft {
	case INIT:
		break
	case SCHEMA_TABLESPACE:
		break
	case SCHEMA_CREATE:
		atomic.AddUint64(&o.global.schema_counter, 1)
		o.process_database_filename(filename)
		if o.DB != "" {
			ft = DO_NOT_ENQUEUE
			o.m_remove(o.global.directory, filename)
		}
		break
	case SCHEMA_TABLE:
		atomic.AddUint64(&o.global.schema_counter, 1)
		if !o.process_table_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_VIEW:
		if !o.process_schema_view_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_SEQUENCE:
		if !o.process_schema_sequence_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_TRIGGER:
		if !o.SkipTriggers {
			if !o.process_schema_filename(filename, TRIGGER) {
				return DO_NOT_ENQUEUE
			}
		}
		break
	case SCHEMA_POST:
		// can be enqueued in any order
		if !o.SkipPost {
			if !o.process_schema_filename(filename, POST) {
				return DO_NOT_ENQUEUE
			}
		}
		break
	case CHECKSUM:
		if !o.process_checksum_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		o.global.intermediate_conf.checksum_list = append(o.global.intermediate_conf.checksum_list, filename)
		break
	case METADATA_GLOBAL:
		o.process_metadata_global(filename)
		o.refresh_table_list(o.global.intermediate_conf)
		break
	case DATA:
		if !o.NoData {
			if !o.process_data_filename(filename) {
				return DO_NOT_ENQUEUE
			}
		} else {
			o.m_remove(o.global.directory, filename)
		}
		o.global.total_data_sql_files++
		break
	case RESUME:
		if o.Stream {
			log.Fatalf("We don't expect to find resume files in a stream scenario")
		}
		break
	case IGNORED:
		log.Warnf("Filename %s has been ignored", filename)
		break
	case LOAD_DATA:
		o.release_load_data_as_it_is_close(filename)
		break
	case SHUTDOWN:
		break
	case INCOMPLETE:
		break
	default:
		log.Infof("Ignoring file %s", filename)
		break
	}
	return ft
}

func remove_fifo_file(o *OptionEntries, fifo_name string) {
	o.global.exec_process_id_mutex.Lock()
	filename, _ := o.global.exec_process_id[fifo_name]
	o.global.exec_process_id_mutex.Unlock()
	if filename != "" {
		o.m_remove(o.global.directory, filename)
	}

}

func (o *OptionEntries) process_stream_filename(iflnm *intermediate_filename) {
	var current_ft = o.process_filename(iflnm.filename)
	if current_ft == INCOMPLETE {
		if iflnm.iterations > 5 {
			log.Warnf("Max renqueing reached for: %s", iflnm.filename)
		} else {
			o.intermediate_queue_incomplete(iflnm)
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
		o.refresh_db_and_jobs(current_ft)
	}
}

func (o *OptionEntries) intermediate_thread() {
	o.global.stream_intermediate_thread.Add(1)
	defer o.global.stream_intermediate_thread.Done()
	var iflnm *intermediate_filename
	o.global.start_intermediate_thread.Lock()
	for {
		iflnm = o.global.intermediate_queue.pop().(*intermediate_filename)
		log.Debugf("intermediate_queue -> %s (%d)", iflnm.filename, iflnm.iterations)
		if strings.Compare(iflnm.filename, "END") == 0 {
			if o.global.intermediate_queue.length > 0 {
				log.Debugf("intermediate_queue <- %s (%d)", iflnm.filename, iflnm.iterations)
				o.global.intermediate_queue.push(iflnm)
				continue
			}
			iflnm = nil
			break
		}
		o.process_stream_filename(iflnm)
		if iflnm == nil {
			break
		}
	}
	var n uint
	for n = 0; n < o.NumThreads; n++ {
	}
	log.Infof("Intermediate thread ended")
	o.refresh_db_and_jobs(INTERMEDIATE_ENDED)
	return
}
