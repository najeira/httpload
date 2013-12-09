package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	//"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sync"
	"time"
)

type Action struct {
	Url            *url.URL
	Method         string
	Header         http.Header
	Body           []byte
	BodyReadCloser io.ReadCloser
	BodyLength     int64
}

type Config struct {
	Actions        []*Action
	Requests       int
	Clients        int
	Duration       int
	KeepAlive      bool
	Compression    bool
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

type Client struct {
	HttpClient      *http.Client
	Requests        int
	Success         int
	NetworkFailed   int
	HttpFailed      int
	Statuses        map[int]int
	ReadThroughput  int64
	WriteThroughput int64
	CountNewConn    int
}

type Conn struct {
	net.Conn
	Client       *Client
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

func NewConn(client *Client, network, addr string, connectTimeout, readTimeout, writeTimeout time.Duration) (*Conn, error) {
	client.CountNewConn++
	var conn net.Conn
	var err error
	if connectTimeout > 0 {
		conn, err = net.DialTimeout(network, addr, connectTimeout)
	} else {
		conn, err = net.Dial(network, addr)
	}
	if err != nil {
		return nil, err
	}
	if readTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(readTimeout))
	}
	if writeTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	}
	this := &Conn{
		Conn:         conn,
		Client:       client,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	return this, nil
}

func (this *Conn) Read(b []byte) (n int, err error) {
	len, err := this.Conn.Read(b)
	if err == nil {
		this.Client.ReadThroughput += int64(len)
		if this.ReadTimeout > 0 {
			this.Conn.SetReadDeadline(time.Now().Add(this.ReadTimeout))
		}
	}
	return len, err
}

func (this *Conn) Write(b []byte) (n int, err error) {
	len, err := this.Conn.Write(b)
	if err == nil {
		this.Client.WriteThroughput += int64(len)
		if this.WriteTimeout > 0 {
			this.Conn.SetWriteDeadline(time.Now().Add(this.WriteTimeout))
		}
	}
	return len, err
}

func NewDial(client *Client, connectTimeout, readTimeout, writeTimeout time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		return NewConn(client, network, addr, connectTimeout, readTimeout, writeTimeout)
	}
}

func NewHttpClient(client *Client, connectTimeout, readTimeout, writeTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			Dial:            NewDial(client, connectTimeout, readTimeout, writeTimeout),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
}

func NewClient(config *Config) *Client {
	client := &Client{
		Requests:        0,
		Success:         0,
		NetworkFailed:   0,
		HttpFailed:      0,
		Statuses:        make(map[int]int),
		ReadThroughput:  0,
		WriteThroughput: 0,
		CountNewConn:    0,
	}
	client.HttpClient = NewHttpClient(
		client, config.ConnectTimeout, config.ReadTimeout, config.WriteTimeout)
	return client
}

func NewHttpRequest(method string, u *url.URL, body io.ReadCloser, bodyLength int64) (*http.Request, error) {
	req := &http.Request{
		Method:     method,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       body,
		Host:       u.Host,
	}
	if body != nil {
		req.ContentLength = bodyLength
	}
	return req, nil
}

func NewRequest(action *Action, config *Config) (*http.Request, error) {
	req, err := NewHttpRequest(action.Method, action.Url, action.BodyReadCloser, action.BodyLength)
	if err != nil {
		return nil, err
	}
	if action.Header != nil {
		for key, value := range action.Header {
			req.Header[key] = value
		}
	}
	if config.KeepAlive {
		req.Header.Set("Connection", "keep-alive")
	} else {
		req.Header.Set("Connection", "close")
	}
	if config.Compression {
		req.Header.Set("Accept-Encoding", "gzip,deflate")
	}
	return req, nil
}

func (this *Client) Do(req *http.Request) {
	res, err := this.HttpClient.Do(req)
	this.Requests++
	if err != nil {
		//log.Fatal(err)
		this.NetworkFailed++
	} else if res == nil {
		//log.Fatal("response is nil")
		this.NetworkFailed++
	} else {
		defer res.Body.Close()
		_, errReadBody := ioutil.ReadAll(res.Body)
		if errReadBody != nil {
			this.NetworkFailed++
		} else {
			this.Statuses[res.StatusCode]++
			if 200 <= res.StatusCode && res.StatusCode < 300 {
				this.Success++
			} else {
				this.HttpFailed++
			}
		}
	}
}

func (this *Client) Run(count int, config *Config, done *sync.WaitGroup) {
	actionCount := len(config.Actions)
	for i := 0; i < count; i++ {
		actionIndex := i % actionCount
		action := config.Actions[actionIndex]
		req, err := NewRequest(action, config)
		if err != nil {
			//log.Fatal(err, req)
			this.NetworkFailed++
		} else {
			this.Do(req)
		}
	}
	done.Done()
}

func (config *Config) Run() {
	fmt.Println("starting benchmark...")

	// create clients
	clients := make([]*Client, config.Clients)
	for i := 0; i < config.Clients; i++ {
		clients[i] = NewClient(config)
	}

	fmt.Printf("%d clients created\n", config.Clients)

	// init wait group
	var done sync.WaitGroup
	done.Add(config.Clients)

	// start clients
	requestsPerClient := config.Requests / config.Clients
	startTime := time.Now()
	for i, client := range clients {
		additional := 0
		if i == 0 {
			additional = config.Requests % config.Clients
		}
		go client.Run(requestsPerClient+additional, config, &done)
	}

	fmt.Printf("%d clients started\n", config.Clients)

	// wait for all clients
	done.Wait()

	elapsed := time.Since(startTime)

	fmt.Println("...ended benchmark")

	printResults(clients, elapsed)
}

func NewConfig() *Config {
	// parse flags
	var requests int
	var clients int
	var path string
	var filename string
	var keepAlive bool
	var compression bool
	var connectTimeout int
	var readTimeout int
	var writeTimeout int
	flag.IntVar(&requests, "r", 0, "Number of requests")
	flag.IntVar(&clients, "c", 1, "Number of concurrent clients")
	flag.StringVar(&path, "u", "", "URL")
	flag.StringVar(&filename, "f", "", "Config file")
	flag.BoolVar(&keepAlive, "k", true, "Use HTTP keep-alive")
	flag.BoolVar(&compression, "g", true, "Use response compression")
	flag.IntVar(&connectTimeout, "tc", 0, "Connect timeout (in milliseconds)")
	flag.IntVar(&readTimeout, "tr", 0, "Read timeout (in milliseconds)")
	flag.IntVar(&writeTimeout, "tw", 0, "Write timeout (in milliseconds)")
	flag.Parse()

	if requests <= 0 {
		fmt.Println("Number of requests required.")
		flag.Usage()
		os.Exit(1)
	}

	if path == "" && filename == "" {
		fmt.Println("URL or Config file required.")
		flag.Usage()
		os.Exit(1)
	}

	// TODO: reading actions from file

	u, err := url.Parse(path)
	if err != nil {
		panic(fmt.Errorf("invalid url: %v", path))
	}

	var actions []*Action

	action := &Action{
		Url:        u,
		Method:     "GET",
		Header:     nil,
		Body:       nil,
		BodyLength: 0,
	}
	actions = []*Action{action}

	config := &Config{
		Actions:        actions,
		Requests:       requests,
		Clients:        clients,
		Duration:       0,
		KeepAlive:      keepAlive,
		Compression:    compression,
		ConnectTimeout: time.Duration(connectTimeout) * time.Millisecond,
		ReadTimeout:    time.Duration(readTimeout) * time.Millisecond,
		WriteTimeout:   time.Duration(writeTimeout) * time.Millisecond,
	}

	return config
}

func printResults(clients []*Client, elapsed time.Duration) {
	var requests int
	var success int
	var networkFailed int
	var httpFailed int
	var readThroughput int64
	var writeThroughput int64
	var countNewConn int
	statuses := make(map[int]int)

	for _, client := range clients {
		requests += client.Requests
		success += client.Success
		networkFailed += client.NetworkFailed
		httpFailed += client.HttpFailed
		readThroughput += client.ReadThroughput
		writeThroughput += client.WriteThroughput
		countNewConn += client.CountNewConn
		for status, count := range client.Statuses {
			statuses[status] += count
		}
	}

	if elapsed == 0 {
		elapsed = time.Millisecond
	}
	seconds := elapsed.Seconds()

	reqsPerSec := int(math.Floor((float64(requests) / seconds) + 0.5))
	readPerSec := int(math.Floor((float64(readThroughput) / seconds) + 0.5))
	writePerSec := int(math.Floor((float64(writeThroughput) / seconds) + 0.5))

	fmt.Println()
	fmt.Printf("Time:                %10.3f secs\n", seconds)
	fmt.Printf("Requests:            %10d hits\n", requests)
	fmt.Printf("Successful requests: %10d hits\n", success)
	fmt.Printf("Network failed:      %10d hits\n", networkFailed)
	fmt.Printf("Http failed:         %10d hits\n", httpFailed)
	for status, count := range statuses {
		fmt.Printf(" Status %3d:         %10d hits\n", status, count)
	}
	fmt.Printf("Request throughput:  %10d reqs/sec\n", reqsPerSec)
	fmt.Printf("Read throughput:     %10d bytes/sec\n", readPerSec)
	fmt.Printf("Write throughput:    %10d bytes/sec\n", writePerSec)
	fmt.Printf("Read total bytes:    %10d bytes\n", readThroughput)
	fmt.Printf("Write total bytes:   %10d bytes\n", writeThroughput)
	fmt.Printf("Connections:         %10d connections\n", countNewConn)
}

func main() {
	if os.Getenv("GOMAXPROCS") == "" {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	config := NewConfig()
	config.Run()
}
