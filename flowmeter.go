package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var logger *log.Logger

var configFile = "config.json"

type flow struct {
	Name   string
	Expire uint
}
type flows struct {
	ImplicitCreate  bool `json:"_implicitCreate"`
	DefaultExpire   uint `json:"_defaultExpire"`
	PredefinedFlows []flow
}

//var config interface{}
// fields have to start with capital letter to be unmarshalled (marked it as public field)
type Config struct {
	// ip and udp port to receive data
	ReceiveIP   string
	ReceivePort int
	// ip and tcp port for answering http requests about flows properties
	HttpIP   string
	HttpPort int
	// configs of different flows
	Flows flows
}

var config Config

// ports default values comes from T9 keyboard and words "flow" and "meter" (port "meter"->63837 is over 49151 and thus not very compliant but it works and nice)
const defaultConfig = `{
    "receiveIP": "127.0.0.1",
    "receivePort": 3569,
    "httpIP": "127.0.0.1",
    "httpPort": 63837,
    "flows": {
        "_implicitCreate": true,
        "_defaultExpire": 86400
    }
}`

type datapointsGroup struct {
	count uint
	sum   float64
}

// here we will store incoming flow datapoints
// keys are strings; values are arrays of datapointGroup's
var flowData = map[string][]datapointsGroup{}

func initFlow(name string, expire uint) {
	capacity := config.Flows.DefaultExpire
	if expire > 0 {
		capacity = expire
	}
	flowData[name] = make([]datapointsGroup, capacity)
}

func init() {
	// load config
	usingDefaultConfig := false
	var configText []byte
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		usingDefaultConfig = true
		//fmt.Fprintf(os.Stderr, "there is no config file [%s], will use defaults:\n%s\n", configFile, defaultConfig)
		configText = []byte(defaultConfig)
	} else {
		configText, err = ioutil.ReadFile(configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't open config [%s]: %v\n", configFile, err)
			os.Exit(1)
		}
	}
	err := json.Unmarshal(configText, &config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't parse config [%s]: %v\n", configFile, err)
		os.Exit(1)
	}
	//fmt.Printf("parsed config: %+v\n", config)

	// preallocate predefined data arrays
	for _, predefinedFlow := range config.Flows.PredefinedFlows {
		initFlow(predefinedFlow.Name, predefinedFlow.Expire)
	}
	//fmt.Printf("flowData init value: %+v\n", flowData)

	// setup logger
	mypath := strings.Split(os.Args[0], "/")
	myname := mypath[len(mypath)-1]
	logFile := myname + ".log"

	logHandle, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't open log [%s]: %v\n", logFile, err)
		os.Exit(1)
	}
	logger = log.New(logHandle, "", log.Ldate|log.Ltime|log.Lshortfile)

	if usingDefaultConfig {
		logger.Printf("there is no config file [%s], will use defaults:\n%s", configFile, string(defaultConfig))
	}
}

func main() {
	logger.Print("flowmeter starting...")

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
	logger.Printf("listening udp on %s:%d", config.ReceiveIP, config.ReceivePort)
	// wrap infinite loop into func and send it to goroutine to be able to also listen http port
	go func() {
		for {
			receiveData(udpConn)
		}
	}()
	// requests server (HTTP)
	// without wrapping in goroutine, http.ListenAndServe block unless there are error, so I can't log about listening http
	go func() {
		http.HandleFunc("/", httpStatus)
		http.HandleFunc("/status", httpStatus)
		http.HandleFunc("/meter", httpMeter)
		err = http.ListenAndServe(config.HttpIP+":"+fmt.Sprintf("%d", config.HttpPort), nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "can't start http server: %v\n", err)
			os.Exit(1)
		}
	}()
	logger.Printf("listening http on %s:%d", config.HttpIP, config.HttpPort)

	// manual blocking to prevent program from immediately ending
	select {}

	logger.Print("flowmeter stopped")
}

func receiveData(conn *net.UDPConn) {
	// 3 seconds read timeout. Any Read call after given time will return with error.
	// FIX: we shouldn't use timeout for network daemons, we should block until some data arrives
	//conn.SetReadDeadline(time.Now().Add(3 * time.Second))

	const maxPayload = 512 // max payload size. UDP by itself allows packets up to 64k bytes
	var payload [maxPayload + 1]byte
	n, err := conn.Read(payload[0:])
	if err != nil {
		logger.Printf("udp read error: %v", err)
		return
	}
	if n > maxPayload {
		logger.Printf("payload [%v] longer than max payload size [%d], rejecting", string(payload[0:n]), maxPayload)
		return
	}

	data := strings.SplitN(string(payload[0:n]), " ", 2)
	if len(data) < 2 {
		logger.Printf("broken udp payload [%s]", string(payload[0:n]))
		return
	}
	flowName := data[0]
	value, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		logger.Printf("can't parse value [%s] into float64: %v", data[1], err)
		return
	}

	logger.Printf("received value [%.3f] for flow [%s]", value, flowName)

	if _, exists := flowData[flowName]; exists {
		// ok
	} else if config.Flows.ImplicitCreate {
		logger.Printf("received unknown flow [%s], implicitly adding to storage with expire [%d] seconds", flowName, config.Flows.DefaultExpire)
		initFlow(flowName, 0) // 0 means `use default expire value`
	} else {
		logger.Printf("can't store data: flow [%s] is unknown and implicit flow creation is disabled", flowName)
		return
	}

	// proceed adding data

	// disable timeout
	//conn.SetReadDeadline(time.Time{})
}

func httpStatus(writer http.ResponseWriter, req *http.Request) {
	writer.Write([]byte("I'm fine, thanks!\n"))
}

func httpMeter(writer http.ResponseWriter, req *http.Request) {

}
