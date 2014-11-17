package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

var logger *log.Logger

var configFile = "config.json"

//var config interface{}
type Config struct {
	Port string // have to start with capital letter to be unmarshalled (mark it as public field)
}

var config Config
var defaultConfig = []byte(`{
    "port": "3569"
}`)

func _init() {
	// load config
	var configText []byte
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "there is no config file [%s], will use defaults\n", configFile)
		configText = defaultConfig
	} else {
		configText, err = ioutil.ReadFile(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't open config [%v]: %v\n", configFile, err)
			os.Exit(1)
		}
	}
	err := json.Unmarshal(configText, &config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't parse config [%v]: %v\n", configFile, err)
	}
	fmt.Printf("parsed config: %+v\n", config)

	// setup logger
	mypath := strings.Split(os.Args[0], "/")
	myname := mypath[len(mypath)-1]
	logFile := myname + ".log"

	logHandle, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't open log [%v]: %v\n", logFile, err)
		os.Exit(1)
	}
	logger = log.New(logHandle, "", log.Ldate|log.Ltime|log.Lshortfile)
}

func main() {
	_init()

	logger.Print("Start")

	logger.Print("End")
}
