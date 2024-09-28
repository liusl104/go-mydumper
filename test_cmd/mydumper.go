package main

import (
	log "github.com/sirupsen/logrus"
	mydumper "go-mydumper/mydumper"
	"net/http"
)
import _ "net/http/pprof"

func init() {
	go func() {
		log.Println(http.ListenAndServe("localhost:8080", nil))
	}()
}

func main() {
	err := mydumper.CommandDump()
	if err != nil {
		log.Fatal(err)
	}
}
