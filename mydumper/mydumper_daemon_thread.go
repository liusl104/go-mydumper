package mydumper

import (
	"github.com/sevlyar/go-daemon"
	"github.com/siddontang/go-log/log"
	"os"
)

func runDaemon(o *OptionEntries) *daemon.Context {
	cntxt := &daemon.Context{
		PidFileName: o.Daemon.PidFile,
		PidFilePerm: 0600,

		WorkDir: o.global.dump_directory,
		Umask:   027,
		Args:    os.Args,
	}
	if o.CommonOptionEntries.LogFile != "" {
		cntxt.LogFileName = o.CommonOptionEntries.LogFile
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
