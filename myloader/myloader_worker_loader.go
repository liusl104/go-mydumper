package myloader

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

func initialize_loader_threads(o *OptionEntries, conf *configuration) {
	o.global.init_mutex = g_mutex_new()
	var n uint
	o.global.threads = new(sync.WaitGroup)
	o.global.loader_td = make([]*thread_data, o.Common.NumThreads)
	for n = 0; n < o.Common.NumThreads; n++ {
		th := new(thread_data)
		o.global.threads.Add(1)
		o.global.loader_td[n] = th
		o.global.loader_td[n].conf = conf
		o.global.loader_td[n].thread_id = n + 1
		go loader_thread(o, o.global.loader_td[n])
		conf.ready.pop()
	}

}

func loader_thread(o *OptionEntries, td *thread_data) {
	defer o.global.threads.Done()
	var err error
	var conf = td.conf
	o.global.init_mutex.Lock()
	td.thrconn = nil
	o.global.init_mutex.Unlock()
	td.current_database = ""
	td.thrconn, err = m_connect(o)
	if err != nil {
		log.Fatalf("connect database fail:%v", err)
	}
	execute_gstring(td.thrconn, o.global.set_session)
	conf.ready.push(1)
	if o.Common.DB != "" {
		td.current_database = o.Common.DB
		if execute_use(td) {
			log.Fatalf("Thread %d: Error switching to database `%s` when initializing", td.thread_id, td.current_database)
		}
	}
	log.Debugf("Thread %d: Starting import", td.thread_id)
	process_stream_queue(o, td)
	if td.thrconn != nil {
		err = td.thrconn.Close()
	}
	log.Debugf("Thread %d: ending", td.thread_id)
	return
}

func wait_loader_threads_to_finish(o *OptionEntries) {
	o.global.threads.Wait()
	restore_job_finish(o)
}

func inform_restore_job_running(o *OptionEntries) {
	if o.global.shutdown_triggered {
		var n, sum, prev_sum uint
		for n = 0; n < o.Common.NumThreads; n++ {
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
			for n = 0; n < o.Common.NumThreads; n++ {
				if o.global.loader_td[n].status == STARTED {
					sum += 1
				}
			}
		}
		fmt.Printf("\nAll loader thread had finished\n")
	}
}

func free_loader_threads(o *OptionEntries) {
	o.global.loader_td = nil
	o.global.threads = nil
}
