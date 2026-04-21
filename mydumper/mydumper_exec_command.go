package mydumper

import (
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
)

var (
	Num_exec_threads    uint = 4
	exec_command_thread []*GThread
	pid_file_table      map[*command]string
	exec_queue          *GAsyncQueue
)

type command struct {
	pid int
	cmd *exec.Cmd
}

func compress_file_gzip(filename string) {
	src, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open %s for gzip compression: %v", filename, err)
	}
	defer src.Close()

	dst, err := os.Create(filename + GZIP_EXTENSION)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", filename+GZIP_EXTENSION, err)
	}
	defer dst.Close()

	w := gzip.NewWriter(dst)
	if _, err = io.Copy(w, src); err != nil {
		log.Fatalf("Failed to gzip compress %s: %v", filename, err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to finalize gzip for %s: %v", filename, err)
	}
}

func compress_file_zstd(filename string) {
	src, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open %s for zstd compression: %v", filename, err)
	}
	defer src.Close()

	dst, err := os.Create(filename + ZSTD_EXTENSION)
	if err != nil {
		log.Fatalf("Failed to create %s: %v", filename+ZSTD_EXTENSION, err)
	}
	defer dst.Close()

	w, err := zstd.NewWriter(dst)
	if err != nil {
		log.Fatalf("Failed to create zstd writer for %s: %v", filename, err)
	}
	if _, err = io.Copy(w, src); err != nil {
		log.Fatalf("Failed to zstd compress %s: %v", filename, err)
	}
	if err = w.Close(); err != nil {
		log.Fatalf("Failed to finalize zstd for %s: %v", filename, err)
	}
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

func process_exec_command(a any) {
	_ = a
	cmdLower := strings.ToLower(strings.TrimSpace(Exec_command))
	isBuiltinGzip := UseInternalCompress && strings.HasPrefix(cmdLower, GZIP)
	isBuiltinZstd := UseInternalCompress && strings.HasPrefix(cmdLower, ZSTD)

	var bin string
	var c_arg []string
	var filenameIdx int

	if !isBuiltinGzip && !isBuiltinZstd {
		arguments := strings.Split(Exec_command, " ")
		bin = arguments[0]
		c_arg = arguments[1:]
		filenameIdx = slices.Index(c_arg, "FILENAME")
	}

	for {
		task := G_async_queue_pop(exec_queue)
		filename := task.(string)
		if len(filename) == 0 {
			break
		}
		log.Debugf("exec command on file: %s", filename)
		if isBuiltinGzip {
			compress_file_gzip(filename)
		} else if isBuiltinZstd {
			compress_file_zstd(filename)
		} else {
			if filenameIdx >= 0 {
				c_arg[filenameIdx] = filename
			}
			exec_this_command(bin, c_arg, filename)
		}
	}
}

func initialize_exec_command() {
	log.Warnf("initialize_exec_command: Started")
	exec_queue = G_async_queue_new("exec_queue")
	exec_command_thread = make([]*GThread, Num_exec_threads)
	var i uint
	pid_file_table = make(map[*command]string)
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i] = M_thread_new("exec_command", process_exec_command, exec_queue, "Exec command thread could not be created")
	}
}

func wait_exec_command_to_finish() {
	var i uint
	for i = 0; i < Num_exec_threads; i++ {
		G_async_queue_push(exec_queue, "")
	}
	for i = 0; i < Num_exec_threads; i++ {
		exec_command_thread[i].Thread.Wait()
	}
}
