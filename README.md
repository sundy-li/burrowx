## burrowx - kafka offset lag monitor,stored by influxdb

A simple, lightweight kafka offset monitor, currently metrics stored by influxdb. Motivated by   [Burrow](https://github.com/linkedin/Burrow), but much faster and cleaner and more stable. burrowx is good integration with influxdb and grafana.


#### DemoView
![burrowx with influxdb and granfana](./doc/demo.png)

#### Install
```shell
$ go get github.com/sundy-li/burrowx
$ cd $GOPATH/src/github.com/sundy-li/burrowx
$ go build && go install
```

#### Running burrowx
``` shell
## new workspace for burrowx
mkdir -p /data/app/burrowx 
## cd to the workspace
cd /data/app/burrowx 
## cp the files to here
cp $GOPATH/bin/burrowx ./
cp -rf $GOPATH/src/github.com/sundy-li/burrowx/config ./
## modify server.json file config and run it
./burrowx
```



#### Test the data

 - Create a new test topic

	``` 
	    bin/kafka-topics.sh --create  --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic test_burrowx_topic
	```
 
 - Produce the data to the topic

	```
		for i in `seq 1 10000`;do echo "33" |  bin/kafka-console-producer.sh   --topic test_burrowx_topic --broker-list localhost:9092 ; sleep 1; done
	```
 
 - Create a consumer to consume the data
 	
 	`$ pip install kafka`
 	

		from kafka import KafkaConsumer
		consumer = KafkaConsumer('test_burrowx_topic', group_id='my_group2')
		for msg in consumer:
			print(msg)

		print("end")

 Then you will find the data in the influxdb database `burrowx` .


#### Grafana query
 
 [sample_json](./grafana-sample.json)
   
 

#### Features
 - Light weight and extremely simple to use, metrics are stored in [influxdb](https://github.com/influxdata/influxdb),  and could be easily viewed on [grafana](https://github.com/grafana/grafana)
 - Only support kafka version >= 0.9.X, which stores the consumer offsets in the topic `__consumer_offsets`,if you are using kafka 0.8.X, try my previous repo `https://github.com/shunfei/Dcmonitor`
 - Base on topic,partitions of a topic are merged into topic
