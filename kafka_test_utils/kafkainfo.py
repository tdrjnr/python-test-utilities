from kazoo.client import KazooClient
import json


class KafkaInfo(object):
    def __init__(self, hosts):
        self.zk = KazooClient(hosts)
        self.zk.start()

    def brokers(self):
        broker_ids = self.zk.get_children('/brokers/ids')
        return [json.loads(self.zk.get('brokers/ids/'+broker_id)[0])['host'] for broker_id in broker_ids]

    def jmxports(self):
        broker_ids = self.zk.get_children('/brokers/ids')
        return [json.loads(self.zk.get('brokers/ids/'+broker_id)[0])['jmx_port'] for broker_id in broker_ids]

    def topics(self):
        return self.zk.get_children('/brokers/topics')

    def partitions(self, topic):
        strs = self.zk.get_children('/brokers/topics/%s/partitions' % topic)
        return map(int, strs)

    def consumers(self):
        return self.zk.get_children('/consumers')

    def topics_for_consumer(self, consumer):
        return self.zk.get_children('/consumers/%s/offsets' % consumer)

    def offset(self, topic, consumer, partition):
        (n, _) = self.zk.get('/consumers/%s/offsets/%s/%d' % (consumer, topic, partition))
        return int(n)
