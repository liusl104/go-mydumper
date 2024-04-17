package mydumper

import (
	log "github.com/sirupsen/logrus"
	"os/exec"
	"slices"
	"strings"
	"sync"
)

type command struct {
	pid int
	cmd *exec.Cmd
}

func exec_this_command(o *OptionEntries, bin string, c_arg []string, filename string) {
	var found bool
	var this *command
	for p, f := range o.global.pid_file_table {
		if f == filename {
			found = true
			this = p
		}
	}
	if !found {
		cmd := exec.Command(bin, c_arg...)
		err := cmd.Start()
		if err != nil {
			log.Fatalf("exec command %s fail:%v", bin, err)
		}
		c := new(command)
		c.pid = cmd.Process.Pid
		c.cmd = cmd
		o.global.pid_file_table[c] = filename
	} else {
		err := this.cmd.Wait()
		if err != nil {
			log.Fatalf("wait child pid %d : %v", this.pid, err)
		}
		delete(o.global.pid_file_table, this)
	}

}

func process_exec_command(o *OptionEntries, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	var arguments = strings.Split(o.global.exec_command, " ")
	var bin = arguments[0]
	var c_arg []string
	c_arg = arguments[1:]
	c := slices.Index(c_arg, "FILENAME")
	for {
		task := o.global.stream_queue.pop()
		filename := task.(string)
		log.Debugf("get exec command : %s %s %s", bin, strings.Join(c_arg, " "), filename)
		if len(filename) == 0 {
			break
		}
		c_arg[c] = filename
		exec_this_command(o, bin, c_arg, filename)
	}

}
func initialize_exec_command(o *OptionEntries) {
	o.global.num_exec_threads = 4
	log.Warnf("initialize_exec_command: Started")
	o.global.stream_queue = g_async_queue_new(o.CommonOptionEntries.BufferSize)
	o.global.exec_command_thread = make([]*sync.WaitGroup, o.global.num_exec_threads)
	var i uint
	o.global.pid_file_table = make(map[*command]string)
	for i = 0; i < o.global.num_exec_threads; i++ {
		o.global.exec_command_thread[i] = new(sync.WaitGroup)
		go process_exec_command(o, o.global.exec_command_thread[i])
	}
}

func wait_exec_command_to_finish(o *OptionEntries) {
	var i uint
	for i = 0; i < o.global.num_exec_threads; i++ {
		o.global.stream_queue.push("")
	}
	for i = 0; i < o.global.num_exec_threads; i++ {
		o.global.exec_command_thread[i].Wait()
	}
}
