package myloader

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
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
	stream_intermediate_thread *GThread
	exec_per_thread_cmd        []string
	intermediate_conf          *configuration
	schema_counter             uint64
)

// initialize_intermediate_queue sets intermediate_conf, creates intermediate_queue and exec_process_id, starts intermediate_thread, and calls initialize_control_job.
func initialize_intermediate_queue(c *configuration) {
	intermediate_conf = c
	intermediate_queue = G_async_queue_new("intermediate_queue")
	exec_process_id = make(map[string]string)
	exec_process_id_mutex = G_mutex_new()
	start_intermediate_thread = G_mutex_new()
	start_intermediate_thread.Lock()
	if Stream != "" {
		start_intermediate_thread.Unlock()
	}
	intermediate_queue_ended = false
	stream_intermediate_thread = M_thread_new("myloader_intermediate", intermediate_thread, nil, "Intermediate thread could not be created")
	initialize_control_job(c)
}

// intermediate_queue_new pushes an intermediate_filename (filename, iterations 0) onto intermediate_queue.
func intermediate_queue_new(filename string) {
	var iflnm = new(intermediate_filename)
	iflnm.filename = filename
	iflnm.iterations = 0
	log.Debugf("intermediate_queue <- %s (%d)", iflnm.filename, iflnm.iterations)
	G_async_queue_push(intermediate_queue, iflnm)
}

// intermediate_queue_end unlocks start_intermediate_thread, pushes END, joins the intermediate thread, and sets intermediate_queue_ended.
func intermediate_queue_end() {
	start_intermediate_thread.Unlock()
	var e = "END"
	intermediate_queue_new(e)
	log.Info("Intermediate queue: Sending END job")
	G_thread_join(stream_intermediate_thread)
	log.Infof("Intermediate thread: SHUTDOWN")
	intermediate_queue_ended = true
}

// intermediate_queue_incomplete increments iterations and re-pushes the filename to intermediate_queue (retry).
func intermediate_queue_incomplete(iflnm *intermediate_filename) {
	iflnm.iterations++
	log.Debugf("intermediate_queue <- %s (%d) incomplete", iflnm.filename, iflnm.iterations)
	G_async_queue_push(intermediate_queue, iflnm)
}

// process_filename determines file_type, runs the matching process_* (e.g. process_metadata_global, process_table_filename), and returns the file_type or DO_NOT_ENQUEUE.
func process_filename(filename string) file_type {
	var ft = get_file_type(filename)
	switch ft {
	case METADATA_GLOBAL:
		process_metadata_global(filename)
		refresh_table_list(intermediate_conf)
		break
	case SCHEMA_TABLESPACE:
		log.Warnf("Tablespace file %s has been ignored. It should be imported manually before restoring", filename)
		break
	case SCHEMA_SEQUENCE:
		if !process_schema_sequence_filename(filename) {
			return DO_NOT_ENQUEUE
		}
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
	case LOAD_DATA:
		release_load_data_as_it_is_close(filename)
		break
	case SCHEMA_VIEW:
		if !process_schema_view_filename(filename) {
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

	case IGNORED:
		log.Warnf("Filename %s has been ignored", filename)
		break
	case RESUME:
		if Stream != "" {
			log.Critical("We don't expect to find resume files in a stream scenario")
		}
		break
	default:
		log.Infof("Ignoring file %s", filename)
		break
	}
	return ft
}

// remove_fifo_file looks up the filename for the fifo in exec_process_id and removes it from the directory.
func remove_fifo_file(fifo_name string) {
	exec_process_id_mutex.Lock()
	filename, _ := exec_process_id[fifo_name]
	exec_process_id_mutex.Unlock()
	if filename != "" {
		M_remove(directory, filename)
	}

}

// intermediate_thread pops filenames from intermediate_queue, calls process_filename, and enroutes to the right queue; exits on END.
func intermediate_thread(c any) {
	_ = c
	var iflnm *intermediate_filename
	start_intermediate_thread.Lock()
	for {
		var task = G_async_queue_pop(intermediate_queue)
		if task == nil {
			break
		}
		iflnm = task.(*intermediate_filename)
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
		i := process_filename(iflnm.filename)
		enroute_into_the_right_queue_based_on_file_type(i)
	}

	log.Infof("Intermediate thread ended")
	refresh_table_list(intermediate_conf)
	enroute_into_the_right_queue_based_on_file_type(INTERMEDIATE_ENDED)
	return
}

/*func process_stream_filename(iflnm *intermediate_filename) {
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
}*/
