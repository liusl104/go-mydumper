package myloader

import (
	"fmt"
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"sync"
	"time"
)

const (
	RESTORE_JOB_RUNNING_INTERVAL = 10
)

var (
	here_is_your_job *GAsyncQueue
	refresh_db_queue *GAsyncQueue
	data_queue       *GAsyncQueue
	threads          []*GThreadFunc
	loader_td        []*thread_data
)

func initialize_loader_threads(conf *configuration) {
	var n uint
	threads = make([]*GThreadFunc, 0)
	loader_td = make([]*thread_data, NumThreads)
	if MaxThreadsPerTable > NumThreads {
		MaxThreadsPerTable = NumThreads
	}
	for n = 0; n < NumThreads; n++ {
		loader_td[n] = new(thread_data)
		threads[n] = G_thread_new("myloader_loader", new(sync.WaitGroup), n)
		initialize_thread_data(loader_td[n], conf, WAITING, n+1, nil)
		go loader_thread(loader_td[n], n)
		G_async_queue_pop(conf.ready)
	}

}

func create_index_job(conf *configuration, dbt *db_table, tdid uint) bool {
	log.Infof("Thread %d: Enqueuing index for table: %s.%s", tdid, dbt.database.real_database, dbt.table)
	var rj *restore_job = new_schema_restore_job("index", JOB_RESTORE_STRING, dbt, dbt.database, dbt.indexes, INDEXES)
	log.Tracef("index_queue <- %v: %s.%s", rj.job_type, dbt.database.real_database, dbt.table)
	G_async_queue_push(conf.index_queue, new_control_job(JOB_RESTORE, rj, dbt.database))
	dbt.schema_state = INDEX_ENQUEUED
	return true
}

func enqueue_index_for_dbt_if_possible(conf *configuration, dbt *db_table) bool {
	if dbt.schema_state == DATA_DONE {
		if dbt.indexes == nil {
			dbt.schema_state = ALL_DONE
			return false
		} else {
			return create_index_job(conf, dbt, 0)
		}
	}
	return dbt.schema_state != ALL_DONE
}

func enqueue_indexes_if_possible(conf *configuration) {
	conf.table_list_mutex.Lock()
	for _, dbt := range conf.table_list {
		dbt.mutex.Lock()
		enqueue_index_for_dbt_if_possible(conf, dbt)
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

func process_loader_thread(td *thread_data) {
	var job *control_job
	var cont bool = true
	var ft file_type = -1
	var rj *restore_job
	var dbt *db_table
	for cont {
		log.Tracef("refresh_db_queue <- %v", THREAD)
		G_async_queue_push(refresh_db_queue, THREAD)
		ft = G_async_queue_pop(here_is_your_job).(file_type)
		log.Tracef("here_is_your_job -> %v", ft)
		switch ft {
		case DATA:
			rj = G_async_queue_pop(data_queue).(*restore_job)
			dbt = rj.dbt
			log.Tracef("data_queue -> %v: %s.%s, threads %u", rj.job_type, dbt.database.real_database, dbt.real_table, dbt.current_threads)
			job = new_control_job(JOB_RESTORE, rj, dbt.database)
			td.dbt = dbt
			cont = process_job(td, job, nil)
			dbt.mutex.Lock()
			dbt.current_threads--
			log.Tracef("%s.%s: done job, threads %d", dbt.database.real_database, dbt.real_table, dbt.current_threads)
			dbt.mutex.Unlock()
			break
		case SHUTDOWN:
			cont = false
			break
		case IGNORED:
			time.Sleep(1000 * time.Millisecond)
			break
		default:
			break
		}
	}
	enqueue_indexes_if_possible(td.conf)
	log.Infof("Thread %d: Data import ended", td.thread_id)
	maybe_shutdown_control_job()
}
func loader_thread(td *thread_data, thread_id uint) {
	defer threads[thread_id].Thread.Done()
	var conf *configuration = td.conf
	G_async_queue_push(conf.ready, 1)
	log.Tracef("Thread %d: Starting import", td.thread_id)
	process_loader_thread(td)
	log.Tracef("Thread %d: ending", td.thread_id)
}

func wait_loader_threads_to_finish() {
	var n uint
	for n = 0; n < NumThreads; n++ {
		threads[n].Thread.Wait()
	}
	restore_job_finish()
}

func inform_restore_job_running() {
	if shutdown_triggered {
		var n, sum, prev_sum uint
		for n = 0; n < NumThreads; n++ {
			if loader_td[n].status == STARTED {
				sum += 1
			}
		}
		fmt.Printf("Printing remaining loader threads every %d seconds", RESTORE_JOB_RUNNING_INTERVAL)
		for sum > 0 {
			if prev_sum != sum {
				fmt.Printf("\nThere are %d loader thread still working", sum)
			} else {
				fmt.Printf(".")
			}
			time.Sleep(RESTORE_JOB_RUNNING_INTERVAL * time.Second)
			prev_sum = sum
			sum = 0
			for n = 0; n < NumThreads; n++ {
				if loader_td[n].status == STARTED {
					sum += 1
				}
			}
		}
		fmt.Printf("\nAll loader thread had finished\n")
	}
}

func free_loader_threads() {
	loader_td = nil
	threads = nil
}
