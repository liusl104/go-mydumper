package myloader

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func (o *OptionEntries) initialize_loader_threads(conf *configuration) {
	var n uint
	o.global.threads = make([]*GThreadFunc, 0)
	o.global.loader_td = make([]*thread_data, o.NumThreads)
	if o.MaxThreadsPerTable > o.NumThreads {
		o.MaxThreadsPerTable = o.NumThreads
	}
	for n = 0; n < o.NumThreads; n++ {
		o.global.loader_td[n] = new(thread_data)
		o.global.threads[n] = g_thread_new("myloader_loader", new(sync.WaitGroup), n)
		initialize_thread_data(o.global.loader_td[n], conf, WAITING, n+1, nil)
		go o.loader_thread(o.global.loader_td[n], n)
		g_async_queue_pop(conf.ready)
	}

}

func create_index_job(conf *configuration, dbt *db_table, tdid uint) bool {
	log.Infof("Thread %d: Enqueuing index for table: %s.%s", tdid, dbt.database.real_database, dbt.table)
	var rj *restore_job = new_schema_restore_job("index", JOB_RESTORE_STRING, dbt, dbt.database, dbt.indexes, INDEXES)
	log.Tracef("index_queue <- %v: %s.%s", rj.job_type, dbt.database.real_database, dbt.table)
	g_async_queue_push(conf.index_queue, new_control_job(JOB_RESTORE, rj, dbt.database))
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

func (o *OptionEntries) enqueue_indexes_if_possible(conf *configuration) {
	conf.table_list_mutex.Lock()
	for _, dbt := range conf.table_list {
		dbt.mutex.Lock()
		enqueue_index_for_dbt_if_possible(conf, dbt)
		dbt.mutex.Unlock()
	}
	conf.table_list_mutex.Unlock()
}

func (o *OptionEntries) process_loader_thread(td *thread_data) {
	var job *control_job
	var cont bool = true
	var ft file_type = -1
	var rj *restore_job
	var dbt *db_table
	for cont {
		log.Tracef("refresh_db_queue <- %v", THREAD)
		g_async_queue_push(o.global.refresh_db_queue, THREAD)
		ft = g_async_queue_pop(o.global.here_is_your_job).(file_type)
		log.Tracef("here_is_your_job -> %v", ft)
		switch ft {
		case DATA:
			rj = g_async_queue_pop(o.global.data_queue).(*restore_job)
			dbt = rj.dbt
			log.Tracef("data_queue -> %v: %s.%s, threads %u", rj.job_type, dbt.database.real_database, dbt.real_table, dbt.current_threads)
			job = new_control_job(JOB_RESTORE, rj, dbt.database)
			td.dbt = dbt
			cont = o.process_job(td, job, nil)
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
	o.enqueue_indexes_if_possible(td.conf)
	log.Infof("Thread %d: Data import ended", td.thread_id)
	o.maybe_shutdown_control_job()
}
func (o *OptionEntries) loader_thread(td *thread_data, thread_id uint) {
	defer o.global.threads[thread_id].thread.Done()
	var conf *configuration = td.conf
	g_async_queue_push(conf.ready, 1)
	log.Tracef("Thread %d: Starting import", td.thread_id)
	o.process_loader_thread(td)
	log.Tracef("Thread %d: ending", td.thread_id)
}

func (o *OptionEntries) wait_loader_threads_to_finish() {
	var n uint
	for n = 0; n < o.NumThreads; n++ {
		o.global.threads[n].thread.Wait()
	}
	o.restore_job_finish()
}

func (o *OptionEntries) inform_restore_job_running() {
	if o.global.shutdown_triggered {
		var n, sum, prev_sum uint
		for n = 0; n < o.NumThreads; n++ {
			if o.global.loader_td[n].status == STARTED {
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
			for n = 0; n < o.NumThreads; n++ {
				if o.global.loader_td[n].status == STARTED {
					sum += 1
				}
			}
		}
		fmt.Printf("\nAll loader thread had finished\n")
	}
}

func (o *OptionEntries) free_loader_threads() {
	o.global.loader_td = nil
	o.global.threads = nil
}
