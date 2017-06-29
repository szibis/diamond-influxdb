# coding=utf-8

"""
Send metrics to a [influxdb](https://github.com/influxdb/influxdb/) using the
http interface.

v1.0 : creation
v1.1 : force influxdb driver with SSL
v1.2 : added a timer to delay influxdb writing in case of failure
       this whill avoid the 100% cpu loop when influx in not responding
       Sebastien Prune THOMAS - prune@lecentre.net
v1.3 : Add support for influxdb 0.9 and allow the ability to timeout requests
v1.5 : Add tags and dimensions support and updates for 1.2+ influxdb.
       Adds retries and fix timeouts with latest python-influxdb
v1.6: Add blacklisted fields support and prefix add.
      Adds more complex dimensions support
v1.7: Add __empty__ feature for merging values to one key. Some Exception
      handling when loading dimensions and tags config jsons
      Fix second level metrics parsing

#### Dependencies
 * [influxdb](https://github.com/influxdb/influxdb-python)

#### Configuration
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
merge_delimiter = ':'
tags = '{"region": "us-east-1","env": "production"}'
dimensions = '{"cpu": ["cpu_name"], "diskspace": ["device_name"], "iostat": ["device"], "network": ["device"], "softirq": ["irq"], "test": ["type", "__remove__"], "elasticsearch": { "segments": ["segments", "type"], "cluster_health": ["type"], "indices": ["index", "type"], "thread_pool": ["type"], "jvm": ["type"], "network": ["type"], "disk": ["type"], "process": ["type"], "cache": ["type"], "transport": ["type"]}}'

## blacklisted is list of non allowed field keys in InfluxDB. Example field time.
## blacklisted_prefix will be added for any blacklisted field. Example output _time=0.33.
## Second elements in dimension depth like dict with lists in dict key level for more complex dimensions support. Example elasticsearch collector demensions.
## if you use __remove__ in dimension then this column tag mapping will be removed

```
"""

from six import integer_types
import time
import sys
from Handler import Handler
import json

try:
    from influxdb.client import InfluxDBClient
except ImportError:
    InfluxDBClient = None

try:
    from influxdb.influxdb08 import InfluxDBClient as InfluxDB08Client
except ImportError:
    InfluxDB08Client = None


class InfluxdbHandler(Handler):
    """
    Sending data to Influxdb using batched format
    """

    def __init__(self, config=None):
        """
        Create a new instance of the InfluxdbeHandler
        """
        # Initialize Handler
        Handler.__init__(self, config)

        # Initialize Options
        if self.config['ssl'] == "True":
            self.ssl = True
        else:
            self.ssl = False
        self.hostname = self.config['hostname']
        self.port = int(self.config['port'])
        self.username = self.config['username']
        self.password = self.config['password']
        self.database = self.config['database']
        self.batch_size = int(self.config['batch_size'])
        self.metric_max_cache = int(self.config['cache_size'])
        self.batch_count = 0
        self.time_precision = self.config['time_precision']
        self.timeout = int(self.config['timeout'])
        self.retries = int(self.config['retries'])
        self.influxdb_version = self.config['influxdb_version']
        self.tags = self.config['tags']
        self.reconnect = int(self.config['reconnect_interval'])
        try:
          self.dimensions = json.loads(self.config['dimensions'])
        except Exception:
          self._throttle_error("InfluxDBHandler ERROR - Invalid dimensions JSON in config")
          sys.exit(1)
        self.merge_delimiter = self.config['merge_delimiter']
        self.blacklisted = self.config['blacklisted']
        self.blacklisted_prefix = self.config['blacklisted_prefix']
        self.using_0_8 = False

        if self.influxdb_version in ['0.8', '.8']:
            if not InfluxDB08Client:
                self.log.error(
                    'influxdb.influxdb08.client.InfluxDBClient import failed. '
                    'Handler disabled')
                self.enabled = False
                return
            else:
                self.client = InfluxDB08Client
                self.using_0_8 = True
        else:
            if not InfluxDBClient:
                self.log.error('influxdb.client.InfluxDBClient import failed. '
                               'Handler disabled')
                self.enabled = False
                return
            else:
                self.client = InfluxDBClient

        # Initialize Data
        self.batch = {}
        self.influx = None
        self.batch_timestamp = time.time()
        self.time_multiplier = 1

        # Set send_count for reconnect
        self.send_count = 0

        # Connect
        self._connect()

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(InfluxdbHandler, self).get_default_config_help()

        config.update({
            'hostname': 'Hostname',
            'port': 'Port',
            'ssl': 'set to True to use HTTPS instead of http',
            'batch_size': 'How many metrics to store before sending to the'
                          ' influxdb server',
            'cache_size': 'How many values to store in cache in case of'
                          ' influxdb failure',
            'username': 'Username for connection',
            'password': 'Password for connection',
            'database': 'Database name',
            'time_precision': 'time precision in second(s), milisecond(ms) or '
                              'microsecond (u)',
            'timeout': 'Number of seconds to wait before timing out a hanging'
                       ' request to influxdb',
            'influxdb_version': 'InfluxDB API version, default 1.2',
            'tags': 'static tags added to each metrics'
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(InfluxdbHandler, self).get_default_config()

        config.update({
            'hostname': 'localhost',
            'port': 8086,
            'ssl': False,
            'username': 'root',
            'password': 'root',
            'database': 'graphite',
            'batch_size': 1,
            'cache_size': 20000,
            'time_precision': 's',
            'timeout': 5,
            'retries': 3,
            'reconnect_interval': 0,
            'influxdb_version': '1.2',
            'tags': '{}',
            'dimensions': '{}',
            'merge_delimiter': ':',
            'blacklisted': '["time"]',
            'blacklisted_prefix': '_',
        })

        return config

    def __del__(self):
        """
        Destroy instance of the InfluxdbHandler class
        """
        self._close()

    def process(self, metric):
        if self.batch_count <= self.metric_max_cache:
            # Add the data to the batch
            self.batch.setdefault(metric.path, []).append(metric)
            self.batch_count += 1
        # If there are sufficient metrics, then pickle and send
        if self.batch_count >= self.batch_size and (
                time.time() - self.batch_timestamp) > (
                2 ** self.time_multiplier):
            # Log
            self.log.debug(
                "InfluxdbHandler: Sending batch sizeof : %d/%d after %fs",
                self.batch_count,
                self.batch_size,
                (time.time() - self.batch_timestamp))
            # reset the batch timer
            self.batch_timestamp = time.time()
            # Send pickled batch
            self._send()
        else:
            self.log.debug(
                "InfluxdbHandler: not sending batch of %d as timestamp is %f",
                self.batch_count,
                (time.time() - self.batch_timestamp))

    def _mangle_dimensions(self, auto_tags):
        """
        Remove dimensions level from parsing
        or
        Merge tag value with next value with key name from next col
        """
        merged_dict = {}
        merge_list = []
        merge_next = 0
        merged_key = ""
        if '__merge__' in auto_tags or '__remove__' in auto_tags:
          for key, value in auto_tags.copy().iteritems():
            if key == '__remove__':
               auto_tags.pop(key)
            elif '__merge__' in key:
               merged_key = key.replace("__merge__","")
               merge_list.append(value)
               merge_next += 1
               auto_tags[key] = []
               auto_tags.pop(key)
            elif '__empty__' in key:
               merge_list.append(value)
               merge_next += 1
               auto_tags.pop(key)
            else:
               pass
          if merge_next > 0:
             auto_tags[merged_key] = str(self.merge_delimiter.join(map(str, merge_list[::-1])))
        for k in auto_tags.keys():
          if k.startswith('__empty__'):
            auto_tags.pop(k)
        return auto_tags

    def _new_value(self, metric_measurement, metric_value):
        """
        New collector name
        """
        return str(metric_measurement + "_" + metric_value)

    def _add_empty(self, dimensions, metric_len):
        add_elements = (metric_len) - len(dimensions)
        if add_elements > 0:
          for element in range(0, add_elements):
              empty = "__empty__" + str(element)
              dimensions.append(empty)
        return dimensions

    def _format_metrics(self):
        """
        Build list of metrics formatted for the influxdb client.
        """
        metrics = []

        if self.using_0_8:
            for path in self.batch:
                metrics.append({
                    "points": [[metric.timestamp, metric.value] for metric in
                               self.batch[path]],
                    "name": path,
                    "columns": ["time", "value"]})
        else:
            for path in self.batch:
                for metric in self.batch[path]:
                    # Cast to float to ensure it's written as a float in
                    # InfluxDB. This prevents future errors where the data type
                    # of a field in InfluxDB is 'int', but we try to write a
                    # float to that field.
                    value = metric.value
                    if isinstance(value, integer_types):
                        value = float(value)

                    try:
                      tags = json.loads(self.tags)
                    except Exception:
                      self._throttle_error("InfluxDBHandler ERROR - Invalid tags JSON in config")
                      sys.exit(1)

                    auto_tags = {}

                    metric_value = metric.getMetricPath()
                    metric_value = metric_value.split(".")
                    metric_measurement = metric.getCollectorPath()
                    metric_len = len(metric_value)

                    if self.tags or self.dimensions:
                        if metric_len == 1:
                           auto_tags['collector'] = metric_measurement
                        elif metric_len > 1:
                           #try:
                              if self.dimensions[metric_measurement]:
                                   if type(self.dimensions[metric_measurement]) is list:
                                      if len(self.dimensions[metric_measurement]) <= metric_len:
                                        dimensions = self._add_empty(self.dimensions[metric_measurement], metric_len)
                                        auto_tags = dict(zip(dimensions, metric_value[:-1]))
                                        auto_tags['collector'] = metric_measurement
                                      else:
                                        auto_tags = dict(zip(self.dimensions[metric_measurement], metric_value))
                                        auto_tags['collector'] = metric_measurement
                                   elif type(self.dimensions[metric_measurement]) is dict:
                                        tag_collector = self._new_value(metric_measurement, metric_value[0])
                                        dict_metric_len = metric_len-1
                                        if dict_metric_len <= 2 and dict_metric_len != 0:
                                           auto_tags['collector'] = tag_collector
                                        elif type(self.dimensions[metric_measurement][metric_value[0]]) is list:
                                           new_value = metric_value[0]
                                           tag_collector = self._new_value(metric_measurement, new_value)
                                           metric_value.pop(0)
                                           if len(self.dimensions[metric_measurement][new_value]) <= dict_metric_len:
                                              dimensions = self._add_empty(self.dimensions[metric_measurement][new_value], metric_len)
                                              auto_tags = dict(zip(dimensions, metric_value[:-1]))
                                           else:
                                             auto_tags = dict(zip(self.dimensions[metric_measurement][new_value], metric_value))
                                             auto_tags['collector'] = tag_collector
                                        else:
                                           self._throttle_error(
                                           "InfluxdbHandler: No defined dimensions for zipping in measurement %s", metric_measurement)
                                           pass
                                   else:
                                     self._throttle_error(
                                     "InfluxdbHandler: No defined dimensions for zipping in measurement %s ", metric_measurement)
                                     break
                           #except Exception:
                           #  self._throttle_error(
                           #  "InfluxdbHandler: No defined dimensions for zipping in measurement %s ", metric_measurement)
                           #  break
                        else:
                           auto_tags = {}

                        if len(auto_tags) > 0:
                           # remove all columns with __remove__
                           # or
                           # concatenate using defined delimiter next column
                           # after __merge__ with name from next column
                           auto_tags = self._mangle_dimensions(auto_tags)

                        # add auto discovered tags with dimensions
                        tags.update(auto_tags)

                        # add host from diamond
                        tags.update(json.loads("{\"host\": \"%s\"}" % (metric.host)))

                        #self.log.info(self.blacklisted)
                        if str(metric_value[-1]) in self.blacklisted:
                           field_key = str(self.blacklisted_prefix) + str(metric_value[-1])
                        else:
                           field_key = str(metric_value[-1])

                        metrics.append({
                            "measurement": metric_measurement,
                            "time": metric.timestamp,
                            "fields": {field_key: value},
                            "tags": tags
                        }),
                    else:
                        metrics.append({
                            "measurement": metric_measurement,
                            "time": metric.timestamp,
                            "fields": {metric_value: value},
                            "tags": {"host": metric.host}
                        }),
        return metrics

    def _send(self):
        """
        Send data to Influxdb. Data that can not be sent will be kept in queued.
        """
        # Check to see if we have a valid socket. If not, try to connect.
        try:
            if self.influx is None:
                self.log.info("InfluxdbHandler: Socket is not connected. "
                               "Reconnecting.")
                self._connect()
            if self.influx is None:
                self.log.info("InfluxdbHandler: Reconnect failed.")
            else:
                # Build metrics.
                metrics = self._format_metrics()

                # Send data to influxdb
                self.log.info("InfluxdbHandler: writing %d series of data",
                               len(metrics))
                self.influx.write_points(metrics,
                                         time_precision=self.time_precision)
                if self.reconnect != 0:
                  self.send_count += 1
                  if self.send_count == self.reconnect:
                    self._close()
                    self.send_count = 0
                else:
                  self.send_count = 0

                # empty batch buffer
                self.batch = {}
                self.batch_count = 0
                self.time_multiplier = 1

        except Exception:
            self._close()
            if self.time_multiplier < 5:
                self.time_multiplier += 1
            self._throttle_error(
                "InfluxdbHandler: Error sending metrics, waiting for %ds.",
                2 ** self.time_multiplier)
            raise

    def _connect(self):
        """
        Connect to the influxdb server
        """

        try:
            # Open Connection
            self.influx = self.client(host=self.hostname, port=self.port,
                                      username=self.username, password=self.password,
                                      database=self.database, ssl=self.ssl,
                                      verify_ssl=False, timeout=self.timeout,
                                      retries=self.retries
                                      )
            # Log
            self.log.info("InfluxdbHandler: Established connection to "
                           "%s:%d/%s.",
                           self.hostname, self.port, self.database)
        except Exception, ex:
            # Log Error
            self._throttle_error("InfluxdbHandler: Failed to connect to "
                                 "%s:%d/%s. %s",
                                 self.hostname, self.port, self.database, ex)
            # Close Socket
            self._close()
            return

    def _close(self):
        """
        Close the socket = do nothing for influx which is http stateless
        """
        self.influx = None
