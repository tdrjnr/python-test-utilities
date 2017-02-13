import os
import pexpect
import pexpect.popen_spawn
import matplotlib.pyplot as pl
import csv
import numpy as np
import tarfile
import urllib2
import inspect
from cd import cd


class JmxTool:
    def __init__(self, host, topic="system_test_topic", metrics="broker", kafka_version="0.9.0.1",
                 scala_version="2.11"):
        with cd(os.path.dirname(inspect.stack()[0][1])):
            self._download_and_extract_kafka(kafka_version, scala_version)

            mbean1 = "'kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=" + topic + "' "
            mbean2 = "'kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=" + topic + "' "
            attributes = "OneMinuteRate "

            if metrics is "cpu":
                mbean1 = "'java.lang:type=OperatingSystem' "
                mbean2 = None
                attributes = "ProcessCpuLoad "  # SystemCpuLoad
            elif metrics is "memory":
                mbean1 = "'java.lang:type=Memory' "
                mbean2 = None
                attributes = "HeapMemoryUsage "

            bin_dir = os.path.join("kafka_" + scala_version + "-" + kafka_version, "bin")
            if self._is_windows():
                run_script = "batch " + os.path.join(bin_dir, "windows", "kafka-run-class.bat")
            else:
                run_script = "bash " + os.path.join(bin_dir, "kafka-run-class.sh")

            command = run_script + (" kafka.tools.JmxTool "
                                    "--object-name " + mbean1)
            if mbean2:
                command = command + "--object-name " + mbean2

            command = (command + "--jmx-url "
                                 "service:jmx:rmi:///jndi/rmi://" + host +
                       "/jmxrmi "
                       "--attributes " + attributes +
                       "--reporting-interval 2000")

            if self._is_windows():
                self.jmxtool = pexpect.popen_spawn.PopenSpawn(command)
            else:
                self.jmxtool = pexpect.spawn(command)

            self.host = host
            self.attributes = attributes.strip()

    def _download_and_extract_kafka(self, kafka_version, scala_version):
        kafka_dir_name = 'kafka_' + scala_version + '-' + kafka_version
        kafka_file_name = kafka_dir_name + '.tgz'
        if not os.path.isfile(kafka_file_name):
            url = 'http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/' + kafka_version + '/' + kafka_file_name
            self._download_file(url)
            if not os.path.isdir('./' + kafka_dir_name):
                tar = tarfile.open(kafka_file_name)
                tar.extractall()
                tar.close()

    @staticmethod
    def _download_file(url):
        file_name = url.split('/')[-1]
        req = urllib2.Request(url, headers={'User-Agent': "Magic Browser"})
        u = urllib2.urlopen(req)
        f = open(file_name, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading Apache Kafka: %s Bytes: %s" % (file_name, file_size)

        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = u.read(block_sz)
            if not buffer:
                break

            file_size_dl += len(buffer)
            f.write(buffer)
            status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
            status += chr(8) * (len(status) + 1)
            print status,
        f.close()

    def get_output(self, plot=True):
        # Get all output
        results = ""
        try:
            for n in range(1, 10):
                results += self.jmxtool.read_nonblocking(size=16777216, timeout=2)
        except:
            pass
            # print("Error reading from JmxTool, buffer too small?")
        if plot:
            self.plot_metrics(results)
        return results

    def plot_metrics(self, results, ylabel="", title="", yscale=1):
        if not ylabel:
            ylabel = self.attributes
        if not title:
            title = self.host
        pl.figure()
        reader = csv.reader(results.splitlines())
        data = []
        for row in reader:
            data.append(row)
        data = np.array(data)
        pl.plot((data[1:, 0].astype(float) - data[1, 0].astype(float)) * 1e-3, data[1:, 1:].astype(float) * yscale)
        pl.xlabel("time [s]")
        pl.ylabel(ylabel)
        pl.title(title)

    @staticmethod
    def _is_windows():
        return os.name == 'nt'

    def __del__(self):
        self.jmxtool.close(force=True)
