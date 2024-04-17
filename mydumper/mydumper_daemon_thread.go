package mydumper

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func newSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setsid: true,
	}
}

const ENV_NAME = "XW_DAEMON_IDX"

// 运行时调用background的次数
var runIdx int = 0

// 守护进程
type daemon struct {
	LogFile     string //日志文件, 记录守护进程和子进程的标准输出和错误输出. 若为空则不记录
	MaxCount    int    //循环重启最大次数, 若为0则无限重启
	MaxError    int    //连续启动失败或异常退出的最大次数, 超过此数, 守护进程退出, 不再重启子进程
	MinExitTime int64  //子进程正常退出的最小时间(秒). 小于此时间则认为是异常退出
}

// 把本身程序转化为后台运行(启动一个子进程, 然后自己退出)
// logFile 若不为空,子程序的标准输出和错误输出将记入此文件
// isExit  启动子加进程后是否直接退出主程序, 若为false, 主程序返回*os.Process, 子程序返回 nil. 需自行判断处理
func run_daemon(logFile string, isExit bool) (*exec.Cmd, error) {
	//判断子进程还是父进程
	runIdx++
	envIdx, err := strconv.Atoi(os.Getenv(ENV_NAME))
	if err != nil {
		envIdx = 0
	}
	if runIdx <= envIdx { //子进程, 退出
		return nil, nil
	}

	//设置子进程环境变量
	env := os.Environ()
	env = append(env, fmt.Sprintf("%s=%d", ENV_NAME, runIdx))

	//启动子进程
	cmd, err := startProc(os.Args, env, logFile)
	if err != nil {
		log.Errorf("%d start child process fail: %v", os.Getpid(), err)
		return nil, err
	} else {
		//执行成功
		log.Infof("main pid %d : start child process successfully : pid -> %d", os.Getpid(), cmd.Process.Pid)
	}

	if isExit {
		os.Exit(0)
	}

	return cmd, nil
}

func newDaemon(logFile string) *daemon {
	return &daemon{
		LogFile:     logFile,
		MaxCount:    0,
		MaxError:    3,
		MinExitTime: 10,
	}
}

// 启动后台守护进程
func (d *daemon) run() {
	//启动一个守护进程后退出
	run_daemon(d.LogFile, true)

	//守护进程启动一个子进程, 并循环监视
	var t int64
	count := 1
	errNum := 0
	for {
		//daemon 信息描述
		info := fmt.Sprintf("daemon (pid:%d; count:%d/%d; errNum:%d/%d):",
			os.Getpid(), count, d.MaxCount, errNum, d.MaxError)
		if errNum > d.MaxError {
			log.Errorf(info, "The child process failed to start too many times and exited.")
			os.Exit(EXIT_FAILURE)
		}
		if d.MaxCount > 0 && count > d.MaxCount {
			log.Warnf(info, "Exit after restarting too many times")
			os.Exit(EXIT_SUCCESS)
		}
		count++

		t = time.Now().Unix() //启动时间戳
		cmd, err := run_daemon(d.LogFile, false)
		if err != nil { //启动失败
			log.Errorf(info, "child process failed to start;", "err:", err)
			errNum++
			continue
		}

		//子进程,
		if cmd == nil {
			log.Infof("children process pid=%d: start running...", os.Getpid())
			break
		}

		//父进程: 等待子进程退出
		err = cmd.Wait()
		dat := time.Now().Unix() - t //子进程运行秒数
		if dat < d.MinExitTime {     //异常退出
			errNum++
		} else { //正常退出
			errNum = 0
		}
		log.Infof("%s 监视到子进程(%d)退出, 共运行了%d秒: %v\n", info, cmd.ProcessState.Pid(), dat, err)
	}
}

func startProc(args, env []string, logFile string) (*exec.Cmd, error) {
	cmd := &exec.Cmd{
		Path:        args[0],
		Args:        args,
		Env:         env,
		SysProcAttr: newSysProcAttr(),
	}

	if logFile != "" {
		stdout, err := os.OpenFile(logFile, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Errorf("pid %d : open log fail:%v", os.Getpid(), err)
			return nil, err
		}
		cmd.Stderr = stdout
		cmd.Stdout = stdout
	}

	err := cmd.Start()
	if err != nil {
		return nil, err
	}

	return cmd, nil
}
