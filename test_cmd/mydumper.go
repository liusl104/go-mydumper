package main

import (
	log "github.com/sirupsen/logrus"
	mydumper "go-mydumper/mydumper"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
)
import _ "net/http/pprof"

func init() {
	go func() {
		log.Println(http.ListenAndServe("127.0.0.1:8080", nil))
	}()
}

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal(err)
	}
	runtime.SetCPUProfileRate(100)
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	err = mydumper.CommandDump()
	if err != nil {
		log.Fatal(err)
	}

}
