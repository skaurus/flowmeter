package main

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var logger *log.Logger

var configFile = "config.json"

type flowConfig struct {
	Name   string
	Expire uint
}
type flowsConfig struct {
	ImplicitCreate  bool `json:"_implicitCreate"`
	DefaultExpire   uint `json:"_defaultExpire"`
	PredefinedFlows []flowConfig
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
	Flows flowsConfig
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

// flowData type is supposed to be used as a ring buffer.
// data stored in datapoints array, head is an active index in that array, capacity is array size.
type flowData struct {
	datapoints []datapointsGroup
	head       uint
	capacity   uint
}

// here we will store incoming flow datapoints
// keys are strings; values are pointers to flowData structs
var flowMap = map[string]*flowData{}

func (fm *flowData) advanceHead() {
	fm.head = (fm.head + 1) % fm.capacity
	// clear previous values in now active index
	fm.datapoints[fm.head] = datapointsGroup{}
}

func (fm *flowData) addData(point float64) {
	fm.datapoints[fm.head].count++
	fm.datapoints[fm.head].sum += point
}

func (fm *flowData) NLastPoints(n uint) (points []datapointsGroup) {
	// we could get no more points than there is
	if n > fm.capacity {
		n = fm.capacity
	}
	// preallocate
	points = make([]datapointsGroup, n)
	// we go this way: get current point, get previous point, previous previous and so on
	// should not forget about wrap over
	for i := uint(0); i < n; i++ {
		// let's generalize that. basically we need (head - i) index on every iteration;
		// but what if that value negative? then we need to add fm.capacity to it;
		// so let's always add it; what if head - i was positive and now we get
		// value over capacity - 1? just do modulo division and get remainder to
		// rid off of any "excess" capacity.
		// (Oooh, I just tested that with Perl you can do modulo division directly on
		//  negative numbers with same results... Keewl. But not in Go. "Go Perl!" ;))
		index := (fm.capacity + fm.head - i) % fm.capacity
		points[i] = fm.datapoints[index]
	}
	return
}

func (fm *flowData) MovingAverage(n uint) (average float64) {
	points := fm.NLastPoints(n)
	count, sum := uint(0), float64(0)
	for _, point := range points {
		count += point.count
		sum += point.sum
	}
	if count == 0 {
		average = float64(0)
	} else {
		average = sum / float64(count)
	}
	return
}

func initFlow(name string, expire uint) {
	capacity := config.Flows.DefaultExpire
	if expire > 0 {
		capacity = expire
	}
	flowMap[name] = &flowData{datapoints: make([]datapointsGroup, capacity), head: 0, capacity: capacity}
}

var timeTicker <-chan time.Time

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

	// preallocate predefined data arrays
	for _, predefinedFlow := range config.Flows.PredefinedFlows {
		initFlow(predefinedFlow.Name, predefinedFlow.Expire)
	}
	//fmt.Printf("flowMap init value: %+v\n", flowMap)

	// init timeTicker (we store flow datapoints grouped by second; storage implemented as a ring buffer;
	// for this we need to move a pointer to an active ring buffer index every second)
	timeTicker = time.Tick(1 * time.Second)
	go func() {
		for _ = range timeTicker {
			//fmt.Println("Tick at", t)
			for _, fm := range flowMap {
				fm.advanceHead()
				//fmt.Printf("%+v\n\n", fm)
			}
		}
	}()
}

var chttp = http.NewServeMux()

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
		// handle everything not handled by others
		http.HandleFunc("/", httpStatus)
		// serve static
		chttp.Handle("/", http.FileServer(http.Dir("./public/")))
		// flow requests
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

	//logger.Printf("received value [%.3f] for flow [%s]", value, flowName)

	if _, exists := flowMap[flowName]; exists {
		// ok
	} else if config.Flows.ImplicitCreate {
		logger.Printf("received unknown flow [%s], implicitly adding to storage with expire [%d] seconds", flowName, config.Flows.DefaultExpire)
		initFlow(flowName, 0) // 0 means `use default expire value`
	} else {
		logger.Printf("can't store data: flow [%s] is unknown and implicit flow creation is disabled", flowName)
		return
	}

	// proceed adding data
	fm := flowMap[flowName]
	fm.addData(value)

	// disable timeout
	//conn.SetReadDeadline(time.Time{})
}

func httpStatus(writer http.ResponseWriter, req *http.Request) {
	if _, err := os.Stat("./public/" + req.URL.Path); os.IsNotExist(err) {
		writer.Write([]byte("I'm fine, thanks!\n"))
	} else {
		chttp.ServeHTTP(writer, req)
	}
}

var meterHTMLTemplate = template.Must(template.New("meter").Parse(`<!doctype html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta http-equiv="X-UA-Compatible" content="IE=edge">
        <title></title>
        <script src="/js/jquery-2.1.1.min.js"></script>
        <script src="/js/highcharts-4.0.4-custom.js"></script>
    </head>
    <body>
{{if .Error}}
    {{print .Error}}
{{else}}
        <div id="container" style="width:700px; height:400px;"></div>
        <script type="text/javascript">
var interval;
$(document).ready(function () {
    Highcharts.setOptions({
        global: {
            useUTC: false
        }
    });

    $('#container').highcharts({
        chart: {
            type: 'spline',
            animation: Highcharts.svg, // don't animate in old IE
            marginRight: 10,
            events: {
                load: function () {
                    // set up the updating of the chart each second
                    var series = this.series[0];
                    interval = setInterval(function () {
                        $.ajax({
                            'url': '/meter',
                            'data': {
                                'flow': '{{.FlowName}}',
                                'window': '{{.Window}}',
                                'format': 'json'
                            },
                            'timeout': 200, // ms
                            'error': function () {},
                            'success': function (data) {
                                if (data.Success) {
                                    series.addPoint([(new Date()).getTime(), data.Data.Average], true, true)
                                } else {
                                    console.log(data.Data.Error)
                                }
                            }
                        });
                    }, 1000);
                }
            }
        },
        title: {
            text: 'flow {{.FlowName}} moving average for {{.Window}} seconds'
        },
        xAxis: {
            type: 'datetime',
            tickPixelInterval: 50
        },
        yAxis: {
            title: {
                text: 'seconds'
            },
            floor: 0
        },
        series: [{
            data: (function () {
                var data = [],
                    time = (new Date()).getTime();
                for (var i = -49; i <= 0; i++) {
                    data.push({
                        x: time + i*1000,
                        y: 0
                    });
                }
                data.push({x: time, y: {{.Average}}});
                return data
            }())
        }]
    });
});
        </script>
{{end}}
    </body>
</html>`))

func httpMeter(writer http.ResponseWriter, req *http.Request) {
	templateFormat := "html"
	if len(req.FormValue("format")) > 0 {
		templateFormat = req.FormValue("format")
	}

	var templateData struct {
		Success bool
		Data    interface{}
	}
	templateData.Success = false

	defer func() {
		var err error
		if templateFormat == "html" {
			if !templateData.Success {
				writer.WriteHeader(http.StatusBadRequest)
			}
			err = meterHTMLTemplate.Execute(writer, templateData.Data)
		} else if templateFormat == "json" {
			var js []byte
			js, err = json.Marshal(templateData)
			writer.Header().Set("Content-Type", "application/json")
			writer.Write(js)
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
			meterHTMLTemplate.Execute(writer, "unknown format")
			err = fmt.Errorf("requested unknown format [%s]", templateFormat)
		}
		if err != nil {
			logger.Print("template error:", err)
		}
	}()

	flowName, windowValue := req.FormValue("flow"), req.FormValue("window")
	if len(flowName) == 0 || len(windowValue) == 0 {
		templateData.Data = struct{ Error string }{"flow and window are required parameters"}
		return
	}

	win, err := strconv.ParseUint(windowValue, 10, 32)
	if err != nil {
		logger.Printf("can't parse value [%s] into uint32: %v", windowValue, err)
		templateData.Data = struct{ Error string }{"window is cannot be converted to uint"}
		return
	}
	window := uint(win)

	if fm, exists := flowMap[flowName]; exists {
		average := fm.MovingAverage(window)
		templateData.Success = true
		templateData.Data = struct {
			Error    string
			Average  float64
			FlowName string
			Window   uint
		}{"", average, flowName, window}
		logger.Printf("%+v", templateData.Data)
		return
	}

	templateData.Data = struct{ Error string }{"unknown flow"}
	return
}
