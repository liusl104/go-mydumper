package myloader

import (
	"fmt"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os"
	"time"
)

const (
	RESTORE_JOB_RUNNING_INTERVAL = 10
)

var (
	// here_is_your_job *GAsyncQueue
	refresh_db_queue *GAsyncQueue
	threads          []*GThread
	loader_td        []*thread_data
)

func initialize_loader_threads(conf *configuration) {
	var n uint
	threads = make([]*GThread, NumThreads)
	loader_td = make([]*thread_data, NumThreads)
	if MaxThreadsPerTable > NumThreads {
		MaxThreadsPerTable = NumThreads
	}
	for n = 0; n < NumThreads; n++ {
		loader_td[n] = new(thread_data)
		initialize_thread_data(loader_td[n], conf, WAITING, n+1, nil)
		threads[n] = M_thread_new("myloader_loader", loader_thread, loader_td[n], "Loader thread could not be created")
		G_async_queue_pop(conf.ready)
	}

}

func process_loader_thread(td *thread_data) {
	var job *control_job
	var cont bool = true
	var ft file_type = -1
	var rj *restore_job
	var dbt *db_table
	for cont {
		ft = request_restore_data_job()
		switch ft {
		case DATA:
			rj = request_next_data_job()
			dbt = rj.dbt
			job = new_control_job(JOB_RESTORE, rj, dbt.database)
			td.dbt = dbt
			cont = process_job(td, job, nil)
			dbt.mutex.Lock()
			dbt.current_threads--
			log.Debugf("%s.%s: done job, threads %d", dbt.database.real_database, dbt.real_table, dbt.current_threads)
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

func loader_thread(c any) {
	td := c.(*thread_data)
	var cnf *configuration = td.conf
	G_async_queue_push(cnf.ready, 1)
	log.Debugf("Thread %d: Starting import", td.thread_id)
	process_loader_thread(td)
	log.Debugf("Thread %d: ending", td.thread_id)
}

func wait_loader_threads_to_finish() {
	var n uint
	for n = 0; n < NumThreads; n++ {
		G_thread_join(threads[n])
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
		fmt.Fprintf(os.Stdout, "Printing remaining loader threads every %d seconds", RESTORE_JOB_RUNNING_INTERVAL)
		for sum > 0 {
			if prev_sum != sum {
				fmt.Fprintf(os.Stdout, "\nThere are %d loader thread still working", sum)
			} else {
				fmt.Fprintf(os.Stdout, ".")
			}
			// 与 C 版本一致：sleep 的单位是秒
			time.Sleep(RESTORE_JOB_RUNNING_INTERVAL * time.Second)
			prev_sum = sum
			sum = 0
			for n = 0; n < NumThreads; n++ {
				if loader_td[n].status == STARTED {
					sum += 1
				}
			}
		}
		fmt.Fprintf(os.Stdout, "\nAll loader thread had finished\n")
	}
}

func free_loader_threads() {
	loader_td = nil
	threads = nil
}
