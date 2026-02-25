package mydumper

import (
	. "github.com/liusl104/go-mydumper/src"
	log "github.com/liusl104/go-mydumper/src/logrus"
	"github.com/sevlyar/go-daemon"
	"os"
)

var (
	SnapshotInterval int = 60
	SnapshotCount    int = 2
)

// initialize_daemon_thread prepares daemon-related state (currently a no-op).
func initialize_daemon_thread() {

}

// runDaemon reborns the process as a daemon with the configured PidFile, WorkDir, and optional LogFile; returns nil in the child.
func runDaemon() *daemon.Context {
	cntxt := &daemon.Context{
		PidFileName: PidFile,
		PidFilePerm: 0600,

		WorkDir: dump_directory,
		Umask:   027,
		Args:    os.Args,
	}
	if LogFile != "" {
		cntxt.LogFileName = LogFile
		cntxt.LogFilePerm = 0644
	}
	d, err := cntxt.Reborn()
	if err != nil {
		log.Fatal("Unable to run: ", err)
	}
	if d != nil {
		return nil
	}
	log.Print("- - - - - - - - - - - - - - -")
	log.Print("daemon started")
	/*	defer cntxt.Release()*/
	return cntxt
}
