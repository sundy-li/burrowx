## burrowx - kafka offset lag monitor,stored by influxdb

A simple, lightweight kafka offset monitor, currently metrics stored by influxdb. Motivated by   [Burrow](https://github.com/linkedin/Burrow), but much faster and cleaner and more stable. burrow is good integration with influxdb and grafana.


#### DemoView
![./doc/demo.png](burrowx with influxdb and granfana)

#### Install
```
$ go get github.com/sundy-li/burrowx
$ go build && go install
```

#### Running burrowx
```
$ $GOPATH/bin/burrowx --config path/to/server.json
```


#### Grafana query

 - logsize template query  : `SELECT "logsize" FROM "consumer_metrics" WHERE "topic" = '$topic' AND "consumer_group" = '$topic' AND "consumer_group" = '$cluster' AND $timeFilter`  
 
 - offsize template query  : `SELECT "offsize" FROM "consumer_metrics" WHERE "topic" = '$topic' AND "consumer_group" = '$topic' AND "consumer_group" = '$cluster' AND $timeFilter`  

 - lag template query  : `SELECT "lag" FROM "consumer_metrics" WHERE "topic" = '$topic' AND "consumer_group" = '$topic' AND "consumer_group" = '$cluster' AND $timeFilter`  



#### Features
 - Light weight and extremely simple to use, metrics and stored in [influxdb](https://github.com/influxdata/influxdb),  and could be easily viewed on [grafana](https://github.com/grafana/grafana)
 - Only support kafka version 0.9.X,0.10.X which stores the consumer offsets in kakfa topic `_consumer_offsets`
 - Base on topic,partitions of a topic are merged into topic
