from kazoo.client import KazooClient
import json


class KafkaInfo(object):
    """
    Get information on a Kafka cluster from its Zookeeper
    """

    def __init__(self, hostname):
        """
        :param hostname: hostname of a zookeeper
        Specifying the port is optional, will try default of 2181
        """
        self.zk = KazooClient(hostname)
        self.zk.start()

    def brokers(self):
        """
        :return: list of hostnames of the Kafka brokers
        """
        broker_ids = self.zk.get_children('/brokers/ids')
        return [json.loads(self.zk.get('brokers/ids/'+broker_id)[0])['host'] for broker_id in broker_ids]

    def jmxports(self):
        """
        :return: list of jmx ports of the Kafka brokers
        """
        broker_ids = self.zk.get_children('/brokers/ids')
        return [json.loads(self.zk.get('brokers/ids/'+broker_id)[0])['jmx_port'] for broker_id in broker_ids]

    def topics(self):
        """
        :return: list of topics on the Kafka cluster
        """
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
