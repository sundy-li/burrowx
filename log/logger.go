package log

import log "github.com/cihub/seelog"

//return main file path
func InitLogger(cfgfile string) {
	logger, err := log.LoggerFromConfigAsFile(cfgfile)
	if err != nil {
		panic(err)
	}
	log.ReplaceLogger(logger)
}
