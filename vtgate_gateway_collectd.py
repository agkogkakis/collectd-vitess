#!/usr/bin/python

import time
import util
import mock

NAME = 'vtgate'

#Module that generates metric using the vtgate /debug/gateways endpoint
#Metrics generated
# For all keyspace shards  servingMaster: 1/0 indicating whether there is a Master tablet up and serving
class VtgateGateway(util.BaseCollector):
    def __init__(self, collectd, json_provider=None, verbose=False, interval=None):
        super(VtgateGateway, self).__init__(collectd, NAME, 15001, json_provider, verbose, interval)

    def configure_callback(self, conf):
        super(VtgateGateway, self).configure_callback(conf)
        self.register_read_callback()

    def process_data(self, json_data):
       for keyspaceShardTabletType, tablets in json_data.items():
          if "MASTER" in keyspaceShardTabletType:
            foundServing = False
            for tablet in tablets['TabletsStats']:
                if tablet['Up'] and tablet['Serving']:
                    foundServing = True
                    break
            self.emitter.emit("servingMaster", 1 if foundServing else 0, 'gauge', {"ks": tablets['Target']['keyspace'], "shard": tablets['Target']['shard']})
            

if __name__ == '__main__':
    util.run_local(NAME, VtgateGateway)
else:
    import collectd
    vt = VtgateGateway(collectd)
    collectd.register_config(vt.configure_callback)
