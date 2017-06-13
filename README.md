# diamond-influxdb
This is extended version of InfluxDB Handler for Diamond (https://github.com/python-diamond)

All this features helping as to move from graphite to influx as metrics store on production.
We can now move all traffic to graphite and influxdb in parallel without any templating/proxy between.

Based on influxdb handler version from https://github.com/python-diamond/Diamond/pull/504

* Support for latest InfluxDB OSS/Enterprise 1.2.x+
* Works on latest python-influxdb 4.x
* Add static tags like prefix in Diamond but as key:value format used in InfluxDB
* Autodiscovery and dimensionalized tags with some static mapping help from config
* Remove unwanted columns in dimensions mapping before they tagged

## Configuration

```
[[InfluxdbHandler]]
hostname = localhost
port = 8086 #8084 for HTTPS
batch_size = 100 # default to 1
cache_size = 1000 # default to 20000
username = root
password = root
database = graphite
time_precision = s
timeout = 5 #timeout in influx client
retries = 3 #number of retries in influx client
reconnct_interval = 5 #reconnect after 5 successful sends
influxdb_version = 1.2
blacklisted = '["time"]'
blacklisted_prefix = '_'
tags = '{"region": "us-east-1","env": "production"}'
dimensions = '{"cpu": ["cpu_name"], "diskspace": ["device_name"], "iostat": ["device"], "network": ["device"], "softirq": ["irq"], "test": ['type', '__remove__'], "elasticsearch": { "segments": ["segments", "type"], "cluster_health": ["type"], "indices": ["index", "type"], "thread_pool": ["type"], "jvm": ["type"], "network": ["type"], "disk": ["type"], "process": ["type"], "cache": ["type"], "transport": ["type"]}}'
```

* ```host``` - Tag is autodiscovered from Diamond internal from ```hostname_method```
* ```tags``` - Static tags are appended to other tags. Format: json with key:value
* ```reconnect_interval``` default no reconnect but if set reconnect then after defined number of send it will reconnect connection. This will fix AWS ELB no reconnect because we never hit idle and it is pinned to single instance all the time.
* ```dimensions``` - This feature will help map columns key's to values exposed in Diamond flat metrics. Any other collector that have only measurment and field will be discovered automatic. Results will be added as key:value to other tags. Second elements in dimension depth like dict with lists in dict key level for more complex dimensions support. Example elasticsearch collector demensions. Format: json - collectors names with mapping list inside
* ```blacklisted``` - blacklisted is list of non allowed field keys in InfluxDB. Example field time.
* ```blacklisted_prefix``` - blacklisted_prefix will be added for any blacklisted field. Example output _time=0.33

If you like to remove column mapping (effective tag remove) from particular dimension mapping just use ```__remove__``` in mapping list on this column and will not be added as tag.

By default we adding ```collector``` tag with name of collector or with ```<collector_name>_<depth_one_name>```. Example: ```collector=elasticsearch_indices```

## Examples

```
iostat,collector=iostat,device=xvda,env=production,host=ip-172-17-115-176,region=us-east-1 writes_merged=8.0 1497340065
elasticsearch,collector=elasticsearch_indices,env=production,host=ip-172-17-115-176,index=test_index,region=us-east-1,type=filter_cache memory_size_in_bytes=150536.0 1497340078
memory,collector=memory,env=production,host=ip-172-17-115-176,region=us-east-1 Shmem=18038784.0 1497340068
tcp,collector=tcp,env=production,host=ip-172-17-115-176,region=us-east-1 TCPDirectCopyFromPrequeue=640.9666666666667 1497340063
tcp,collector=tcp,env=production,host=ip-172-17-115-176,region=us-east-1 TCPFACKReorder=0.0 1497340063
elasticsearch,collector=elasticsearch_indices,env=production,host=ip-172-17-115-176,index=test_index,region=us-east-1,type=get exists_time_in_millis=0.0 1497340078
memory,collector=memory,env=production,host=ip-172-17-115-176,region=us-east-1 CmaTotal=0.0 1497340068
```
* all static tags added to each metric
* host added automatic
* tcp, memory and file have no additional tags
* network collector have additional column which is named in config as ```device``` and it is mapped to ```device:eth0```
