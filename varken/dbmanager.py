from sys import exit
from logging import getLogger
#from influxdb_client import InfluxDBClient
#from requests.exceptions import ConnectionError
#from influxdb.exceptions import InfluxDBServerError

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS


class DBManager(object):
    def __init__(self, server):
        self.server = server
        self.logger = getLogger()
        if self.server.url == "influxdb.domain.tld":
            self.logger.critical("You have not configured your varken.ini. Please read Wiki page for configuration")
            exit()
        
        influx_url = "http://" + self.server.url  + ":" +  str(self.server.port);
        if self.server.ssl:
            influx_url = "https://" + self.server.url + ":" + self.server.port;

        #self.influx = InfluxDBClient(host=self.server.url, port=self.server.port, username=self.server.username,
        #                             password=self.server.password, ssl=self.server.ssl, database='varken',
        #                             verify_ssl=self.server.verify_ssl)
        self.logger.debug('Influx Url: %s', influx_url)  
        self.influx = InfluxDBClient(url=influx_url, token=self.server.token, ogr=self.server.org, bucket=self.server.bucket)

        write_api = self.influx.write_api(write_options=SYNCHRONOUS)

        # try:
        #     version = self.influx.request('ping', expected_response_code=204).headers['X-Influxdb-Version']
        #     self.logger.info('Influxdb version: %s', version)
        # except ConnectionError:
        #     self.logger.critical("Error testing connection to InfluxDB. Please check your url/hostname")
        #     exit(1)

        # databases = [db['name'] for db in self.influx.get_list_database()]

        # if 'varken' not in databases:
        #     self.logger.info("Creating varken database")
        #     self.influx.create_database('varken')

        #     retention_policies = [policy['name'] for policy in
        #                           self.influx.get_list_retention_policies(database='varken')]
        #     if 'varken 30d-1h' not in retention_policies:
        #         self.logger.info("Creating varken retention policy (30d-1h)")
        #         self.influx.create_retention_policy(name='varken 30d-1h', duration='30d', replication='1',
        #                                             database='varken', default=True, shard_duration='1h')

#    def write_points(self, data):
#        d = data
#        self.logger.debug('Writing Data to InfluxDB %s', d)
#        try:
#            self.influx.write_points(d)
#        except (InfluxDBServerError, ConnectionError) as e:
#            self.logger.error('Error writing data to influxdb. Dropping this set of data. '
#                              'Check your database! Error: %s', e)


    def write_points(self,data):
      d = data
      self.logger.debug('Writing Data to InfluxDB %s', d)  
      #try:
        #writeApi = self.influx.createWriteApi();
      writeApi = self.influx.write_api();
      writeApi.write(bucket=self.server.bucket, org=self.server.org, record=data);
      #self.logger.error('Data Written')
      #except(InfluxDBError) as e:
      #  self.logger.error('Error writing data to influxdb. Dropping this set of data. '
      #                        'Check your database! Error: %s', e)