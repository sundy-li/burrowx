package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	. "github.com/sundy-li/burrowx/config"
	"github.com/sundy-li/burrowx/monitor"
	"github.com/wswz/go_commons/log"
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

	log.Infof("burrowx started,using server config:%s, logfile cfg:%s", cfgFile, cfg.General.Logconfig)

	fetcher, err := monitor.NewFetcher(cfg)
	if err != nil {
		panic(err)
	}
	fetcher.Start()

	log.Infof("You could press [Ctrl+c] to stop burrowx\n")

	WaitForExitSign()
	log.Info("signal catched,burrowx will be shutdown")
	fetcher.Stop()
	log.Info("goodbye")
}

func WaitForExitSign() {
	c := make(chan os.Signal, 1)
	//结束，收到ctrl+c 信号
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGHUP)
	<-c
}
