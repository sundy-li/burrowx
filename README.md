## burrowx - kafka offset lag monitor,stored by influxdb

A simple, lightweight kafka offset monitor, currently metrics stored by influxdb. Motivated by   [Burrow](https://github.com/linkedin/Burrow), but much faster and cleaner and more stable. burrow is good integration with influxdb and grafana.

#### Install
```
$ go get github.com/sundy-li/burrowx
$ go build && go install
$ go install
```

#### Running burrowx
```
$ $GOPATH/bin/burrowx --config path/to/server.json
```
