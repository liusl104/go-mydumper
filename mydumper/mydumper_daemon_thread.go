package mydumper

import (
	"github.com/sevlyar/go-daemon"
	"github.com/siddontang/go-log/log"
	. "go-mydumper/src"
	"os"
)

var (
	SnapshotInterval int = 60
	SnapshotCount    int = 2
)

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
