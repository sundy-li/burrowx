package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	. "github.com/sundy-li/burrowx/config"
	mylog "github.com/sundy-li/burrowx/log"
	"github.com/sundy-li/burrowx/monitor"
)

var (
	cfgFile string
)

func init() {
	flag.StringVar(&cfgFile, "config", "config/server.json", "config file path")
	flag.Parse()
}
func main() {
	cfg := ReadConfig(cfgFile)
	mylog.InitLogger(cfg.General.Logconfig)

	log.Printf("burrowx started,using server config:%s, logfile cfg:%s\n", cfgFile, cfg.General.Logconfig)
	log.Printf("You could press [Ctrl+c] to stop burrowx\n")

	fetcher, err := monitor.NewFetcher(cfg)
	if err != nil {
		panic(err)
	}
	fetcher.Start()
	WaitForExitSign()
	fetcher.Stop()
	log.Printf("signal catched,burrowx will be shutdown, goodbye")
}

func WaitForExitSign() {
	c := make(chan os.Signal, 1)
	//结束，收到ctrl+c 信号
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)
	<-c
}
