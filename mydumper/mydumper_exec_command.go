package mydumper

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"os/exec"
	"slices"
	"strings"
)

var (
	Num_exec_threads    uint = 4
	exec_command_thread []*GThread
	pid_file_table      map[*command]string
)

type command struct {
	pid int
	cmd *exec.Cmd
}

// exec_this_command starts the given command with args (with FILENAME replaced) or waits for an existing command for that filename to finish.
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

// process_exec_command pops filenames from Stream_queue, replaces FILENAME in exec_command args, and runs exec_this_command; exits on empty filename.
func process_exec_command(a any) {
	_ = a
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

// initialize_exec_command creates Stream_queue and Num_exec_threads worker threads running process_exec_command.
func initialize_exec_command() {
	log.Warnf("initialize_exec_command: Started")
	Stream_queue = G_async_queue_new()
	exec_command_thread = make([]*GThread, Num_exec_threads)
	var i uint
	pid_file_table = make(map[*command]string)
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i] = M_thread_new("exec_command", process_exec_command, Stream_queue, "Exec command thread could not be created")
	}
}

// wait_exec_command_to_finish pushes empty filenames to Stream_queue to signal shutdown, then joins all exec threads.
func wait_exec_command_to_finish() {
	var i uint
	for i = 0; i < Num_exec_threads; i++ {
		G_async_queue_push(Stream_queue, "")
	}
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i].Thread.Wait()
	}
}
