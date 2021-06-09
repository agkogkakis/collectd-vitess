#!/usr/bin/python

import time
import util
import mock
from collections import defaultdict

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
        keyspaces = group_tablets_by_keyspace(json_data)

        for keyspaceName, tablets in keyspaces.items():
            foundServingMaster = False
            if tablets:
                for tabletsForType in tablets:
                    if "MASTER" == tabletsForType["tabletType"]:
                        for tablet in tabletsForType['tabletsStats']:
                            if tablet['Up'] and tablet['Serving']:
                                foundServingMaster = True
                                break
                self.emitter.emit("servingMaster", 1 if foundServingMaster else 0, 'gauge', {"ks": tabletsForType["keyspace"], "shard": tabletsForType["shard"]})


def group_tablets_by_keyspace(json_data):
    keyspaces = defaultdict(list)
    for keyspaceShardTabletType, tablets in json_data.items():
        keyspaceName, tabletType = extract_keyspace_tablet_type(keyspaceShardTabletType)
        if keyspaceName and tabletType:
            try:
                keyspaces[keyspaceName].append({"tabletType": tabletType, "keyspace": tablets["Target"]["keyspace"], "shard": tablets["Target"]["shard"], "tabletsStats": tablets["TabletsStats"]})
            except Exception as e:
                util.log('Error while processing %s/%s %s' % (keyspaceName, tabletType, e))
    return keyspaces


def extract_keyspace_tablet_type(keyspaceShardTabletType):
    try:
        cell, keyspace, shard, tabletType = keyspaceShardTabletType.split('.')
        return keyspace, tabletType
    except ValueError:
      util.log('Error while parsing keyspace/tabletType from %s' % keyspaceShardTabletType)
      return "", ""

if __name__ == '__main__':
    util.run_local(NAME, VtgateGateway)
else:
    import collectd
    vt = VtgateGateway(collectd)
    collectd.register_config(vt.configure_callback)
