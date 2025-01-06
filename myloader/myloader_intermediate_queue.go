package myloader

import (
	log "github.com/sirupsen/logrus"
	. "go-mydumper/src"
	"strings"
	"sync"
	"sync/atomic"
)

type intermediate_filename struct {
	filename   string
	iterations uint
}

var (
	intermediate_queue         *GAsyncQueue
	exec_process_id            map[string]string
	start_intermediate_thread  *sync.Mutex
	exec_process_id_mutex      *sync.Mutex
	intermediate_queue_ended   bool
	stream_intermediate_thread *sync.WaitGroup
	exec_per_thread_cmd        []string
	intermediate_conf          *configuration
	schema_counter             uint64
)

func initialize_intermediate_queue(c *configuration) {
	intermediate_conf = c
	intermediate_queue = G_async_queue_new(BufferSize)
	exec_process_id = make(map[string]string)
	exec_process_id_mutex = G_mutex_new()
	start_intermediate_thread = G_mutex_new()
	start_intermediate_thread.Lock()
	if Stream != "" {
		start_intermediate_thread.Unlock()
	}
	intermediate_queue_ended = false
	stream_intermediate_thread = new(sync.WaitGroup)
	go intermediate_thread()
	if ExecPerThreadExtension != "" {
		// TODO this is ignore
		if ExecPerThread == "" {
			log.Errorf("--exec-per-thread needs to be set when --exec-per-thread-extension is used")
		}
	}
	if ExecPerThread != "" {
		if ExecPerThread[0] != '/' {
			log.Errorf("Absolute path is only allowed when --exec-per-thread is used")
		}
		exec_per_thread_cmd = strings.Split(ExecPerThread, " ")
	}
	initialize_control_job(c)
}

func intermediate_queue_new(filename string) {
	var iflnm = new(intermediate_filename)
	iflnm.filename = filename
	iflnm.iterations = 0
	log.Debugf("intermediate_queue <- %s (%d)", iflnm.filename, iflnm.iterations)
	G_async_queue_push(intermediate_queue, iflnm)
}

func intermediate_queue_end() {
	start_intermediate_thread.Unlock()
	var e = "END"
	intermediate_queue_new(e)
	log.Info("Intermediate queue: Sending END job")
	stream_intermediate_thread.Wait()
	log.Infof("Intermediate thread: SHUTDOWN")
	intermediate_queue_ended = true
}

func intermediate_queue_incomplete(iflnm *intermediate_filename) {
	iflnm.iterations++
	log.Debugf("intermediate_queue <- %s (%d) incomplete", iflnm.filename, iflnm.iterations)
	G_async_queue_push(intermediate_queue, iflnm)
}

func process_filename(filename string) file_type {
	var ft = get_file_type(filename)
	switch ft {
	case INIT:
		break
	case SCHEMA_TABLESPACE:
		break
	case SCHEMA_CREATE:
		atomic.AddUint64(&schema_counter, 1)
		process_database_filename(filename)
		if DB != "" {
			ft = DO_NOT_ENQUEUE
			M_remove(directory, filename)
		}
		break
	case SCHEMA_TABLE:
		atomic.AddUint64(&schema_counter, 1)
		if !process_table_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_VIEW:
		if !process_schema_view_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_SEQUENCE:
		if !process_schema_sequence_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		break
	case SCHEMA_TRIGGER:
		if !SkipTriggers {
			if !process_schema_filename(filename, TRIGGER) {
				return DO_NOT_ENQUEUE
			}
		}
		break
	case SCHEMA_POST:
		// can be enqueued in any order
		if !SkipPost {
			if !process_schema_filename(filename, POST) {
				return DO_NOT_ENQUEUE
			}
		}
		break
	case CHECKSUM:
		if !process_checksum_filename(filename) {
			return DO_NOT_ENQUEUE
		}
		intermediate_conf.checksum_list = append(intermediate_conf.checksum_list, filename)
		break
	case METADATA_GLOBAL:
		process_metadata_global(filename)
		refresh_table_list(intermediate_conf)
		break
	case DATA:
		if !NoData {
			if !process_data_filename(filename) {
				return DO_NOT_ENQUEUE
			}
		} else {
			M_remove(directory, filename)
		}
		total_data_sql_files++
		break
	case RESUME:
		if Stream != "" {
			log.Fatalf("We don't expect to find resume files in a stream scenario")
		}
		break
	case IGNORED:
		log.Warnf("Filename %s has been ignored", filename)
		break
	case LOAD_DATA:
		release_load_data_as_it_is_close(filename)
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

func remove_fifo_file(fifo_name string) {
	exec_process_id_mutex.Lock()
	filename, _ := exec_process_id[fifo_name]
	exec_process_id_mutex.Unlock()
	if filename != "" {
		M_remove(directory, filename)
	}

}

func process_stream_filename(iflnm *intermediate_filename) {
	var current_ft = process_filename(iflnm.filename)
	if current_ft == INCOMPLETE {
		if iflnm.iterations > 5 {
			log.Warnf("Max renqueing reached for: %s", iflnm.filename)
		} else {
			intermediate_queue_incomplete(iflnm)
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
		refresh_db_and_jobs(current_ft)
	}
}

func intermediate_thread() {
	stream_intermediate_thread.Add(1)
	defer stream_intermediate_thread.Done()
	var iflnm *intermediate_filename
	start_intermediate_thread.Lock()
	for {
		iflnm = G_async_queue_pop(intermediate_queue).(*intermediate_filename)
		log.Debugf("intermediate_queue -> %s (%d)", iflnm.filename, iflnm.iterations)
		if strings.Compare(iflnm.filename, "END") == 0 {
			if G_async_queue_length(intermediate_queue) > 0 {
				log.Debugf("intermediate_queue <- %s (%d)", iflnm.filename, iflnm.iterations)
				G_async_queue_push(intermediate_queue, iflnm)
				continue
			}
			iflnm = nil
			break
		}
		process_stream_filename(iflnm)
		if iflnm == nil {
			break
		}
	}
	var n uint
	for n = 0; n < NumThreads; n++ {
	}
	log.Infof("Intermediate thread ended")
	refresh_db_and_jobs(INTERMEDIATE_ENDED)
	return
}
