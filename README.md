# diamond-influxdb
This is extended version of InfluxDB Handler for Diamond (https://github.com/python-diamond)

Based on influxdb handler version from https://github.com/python-diamond/Diamond/pull/504

* Support for latest InfluxDB OSS/Enterprise 1.2.x+
* Works on latest python-influxdb 4.x
* Add static tags like prefix in Diamond but as key:value format used in InfluxDB
* Autodiscovery and dimensionalized tags with some static mapping help from config

## Configuration

```
[[InfluxdbHandler]]
hostname = localhost
port = 8086 #8084 for HTTPS
batch_size = 500 # default to 1
cache_size = 10000 # default to 20000
username = diamond-write
password = blablapassword
database = diamond
time_precision = s
timeout = 5
retries = 3
influxdb_version = 1.2
tags = '{"env": "production", "region": "us-east-1"}'
dimensions = '{"cpu": ["cpu_name"], "fluentd": ["port", "source", "destination"], "diskspace": ["device_name"], "iostat": ["device"], "network": ["device"], "softirq": ["irq"]}'
```

* ```host``` - Tag is autodiscovered from Diamond internal from ```hostname_method```
* ```tags``` - Static tags are appended to other tags
* ```dimensions``` - This feature will help map columns key's to values exposed in Diamond flat metrics. Any other collector that have only measurment and field will be discovered automatic. Results will be added as key:value to other tags
