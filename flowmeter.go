package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	//  "encoding/json"
)

var logger *log.Logger
var config interface{}

func _init() {
	mypath := strings.Split(os.Args[0], "/")
	myname := mypath[len(mypath)-1]
	logfile := myname + ".log"

	loghandle, err := os.OpenFile(logfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't open log [%v]: %v\n", logfile, err)
		os.Exit(1)
	}
	logger = log.New(loghandle, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	_init()

	logger.Print("Start")

	logger.Print("End")
}
