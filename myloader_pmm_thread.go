package mydumper

import "sync"

func pmm_thread(o *OptionEntries, conf *configuration) {
	if o.global.pmmthread == nil {
		o.global.pmmthread = new(sync.WaitGroup)
	}
	o.global.pmmthread.Add(1)
	defer o.global.pmmthread.Done()
}
