package mydumper

func kill_pmm_thread() {

}
func pmm_thread(conf *configuration) {
	defer pmmthread.Thread.Done()
}
