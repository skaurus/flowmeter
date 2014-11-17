package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	//	"time"
)

var logger *log.Logger

var configFile = "config.json"

//var config interface{}
// fields have to start with capital letter to be unmarshalled (marked it as public field)
type Config struct {
	// ip and udp port to receive data; requests about flows properties will be served from port 80
	ReceiveIP   string
	ReceivePort int
}

var config Config
var defaultConfig = []byte(`{
    "receiveIP": "127.0.0.1",
    "receivePort": 3569
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
	//fmt.Printf("parsed config: %+v\n", config)

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

	// bind to ports
	// data receiver
	udpConn, err := net.ListenUDP(
		"udp",
		&(net.UDPAddr{
			IP:   net.ParseIP(config.ReceiveIP),
			Port: config.ReceivePort,
		}),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't bind to udp port [%v]: %v\n", config.ReceivePort, err)
		os.Exit(1)
	}
	defer udpConn.Close()
	logger.Printf("listening on udp port %d", config.ReceivePort)
	for {
		receiveData(logger, udpConn)
	}
	// requests server

	logger.Print("End")
}

func receiveData(logger *log.Logger, conn *net.UDPConn) {
	// 3 seconds read timeout. Any Read call after given time will return with error.
	// NB: we shouldn't use timeout for network daemons, we should block until some data arrives
	//conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	var payload [512]byte // max payload size. UDP by itself allows packets up to 64k bytes
	n, err := conn.Read(payload[0:])
	if err != nil {
		logger.Printf("udp read error: %v", err)
		return
	}

	data := strings.SplitN(string(payload[0:n]), " ", 2)
	if len(data) < 2 {
		logger.Printf("broken udp payload [%s]", string(payload[0:n]))
		return
	}
	param := data[0]
	value := data[1]

	logger.Printf("received value [%s] for param [%s]", value, param)

	// disable timeout
	//conn.SetReadDeadline(time.Time{})
}
