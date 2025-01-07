package mydumper

import (
	. "go-mydumper/src"
	log "go-mydumper/src/logrus"
	"os/exec"
	"slices"
	"strings"
	"sync"
)

var (
	Num_exec_threads    uint = 4
	exec_command_thread []*GThreadFunc
	pid_file_table      map[*command]string
)

type command struct {
	pid int
	cmd *exec.Cmd
}

func exec_this_command(bin string, c_arg []string, filename string) {
	var found bool
	var this *command
	for p, f := range pid_file_table {
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
		pid_file_table[c] = filename
	} else {
		err := this.cmd.Wait()
		if err != nil {
			log.Fatalf("wait child pid %d : %v", this.pid, err)
		}
		delete(pid_file_table, this)
	}

}

func process_exec_command(queue *GAsyncQueue, thread_id uint) {
	_ = queue
	defer exec_command_thread[thread_id].Thread.Done()
	var arguments = strings.Split(exec_command, " ")
	var bin = arguments[0]
	var c_arg []string
	c_arg = arguments[1:]
	c := slices.Index(c_arg, "FILENAME")
	for {
		task := G_async_queue_pop(Stream_queue)
		filename := task.(string)
		log.Debugf("get exec command : %s %s %s", bin, strings.Join(c_arg, " "), filename)
		if len(filename) == 0 {
			break
		}
		c_arg[c] = filename
		exec_this_command(bin, c_arg, filename)
	}

}
func initialize_exec_command() {
	log.Warnf("initialize_exec_command: Started")
	Stream_queue = G_async_queue_new(BufferSize)
	exec_command_thread = make([]*GThreadFunc, Num_exec_threads)
	var i uint
	pid_file_table = make(map[*command]string)
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i] = G_thread_new("exec_command", new(sync.WaitGroup), i)
		go process_exec_command(Stream_queue, i)
	}
}

func wait_exec_command_to_finish() {
	var i uint
	for i = 0; i < Num_exec_threads; i++ {
		G_async_queue_push(Stream_queue, "")
	}
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i].Thread.Wait()
	}
}
