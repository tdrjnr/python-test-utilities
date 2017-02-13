import os
import pexpect
import pexpect.popen_spawn
import matplotlib.pyplot as pl
import csv
import numpy as np
import tarfile
import urllib2


class JmxTool:
    """
    Collect metrics from Kafka brokers via JMX, continuous until object is destroyed
    NB, if not already present in directory this downloads and extracts Kafka package which is ~30MB
    """

    def __init__(self, host, topic="system_test_topic", metrics="broker", kafka_version="0.9.0.1",
                 scala_version="2.11", interval_milliseconds=2000):
        """
        :param host: host address in format <hostname>:<port>
        :param topic: topic name, used if metric is for a specific topic
        :param metrics: metric type, allowed values are 'broker', 'cpu' and 'memory'
        :param kafka_version: specify Kafka version, compatible with version on brokers
        :param scala_version: specify Scala version
        :param interval_milliseconds: interval at which to get a value for the metric
        """
        utils_dir = os.path.dirname(os.path.abspath(__file__))
        self._download_and_extract_kafka(utils_dir, kafka_version, scala_version)

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

        bin_dir = os.path.join(utils_dir,
                               "kafka_" + scala_version + "-" + kafka_version, "bin")
        if self._is_windows():
            run_script = os.path.join(bin_dir, "windows", "kafka-run-class.bat")
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
                   "--reporting-interval " + str(interval_milliseconds))

        if self._is_windows():
            command.replace('\\', '/')
            self.jmxtool = pexpect.popen_spawn.PopenSpawn(command)
        else:
            self.jmxtool = pexpect.spawn(command)

        self.host = host
        self.attributes = attributes.strip()

    def _download_and_extract_kafka(self, extract_to_dir, kafka_version, scala_version):
        file_no_ext = 'kafka_' + scala_version + '-' + kafka_version
        kafka_dir_name = os.path.join(extract_to_dir, file_no_ext)
        kafka_file_name = os.path.join(extract_to_dir, file_no_ext) + '.tgz'
        if not os.path.isfile(kafka_file_name):
            url = 'http://mirror.ox.ac.uk/sites/rsync.apache.org/kafka/' + kafka_version + '/' + file_no_ext + '.tgz'
            self._download_file(url, kafka_file_name)
        if not os.path.isdir(kafka_dir_name):
            tar = tarfile.open(kafka_file_name)
            tar.extractall(path=extract_to_dir)
            tar.close()

    @staticmethod
    def _download_file(url, filename):
        req = urllib2.Request(url, headers={'User-Agent': "Magic Browser"})
        u = urllib2.urlopen(req)
        f = open(filename, 'wb')
        meta = u.info()
        file_size = int(meta.getheaders("Content-Length")[0])
        print "Downloading Apache Kafka: %s Bytes: %s" % (filename, file_size)

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

    def get_output(self):
        """
        Get all output collected since previous call of this function
        :param plot: whether to also plot the output using matplotlib
        :return: collected metric data
        """
        # Get all output
        results = ""
        try:
            for n in range(1, 10):
                results += self.jmxtool.read_nonblocking(size=16777216, timeout=2)
        except:
            pass
            # print("Error reading from JmxTool, buffer too small?")
        plot_handle = self.plot_metrics(results)
        return results, plot_handle

    def plot_metrics(self, results, ylabel="", title="", yscale=1):
        """
        Plot the data
        NB, you'll need to call pylab.show() at the end of your script to see plots
        :param results: data, from get_output
        :param ylabel: defaults to metric type
        :param title: defaults to broker address
        :param yscale: multiplication factor for the y values, defaults to 1
        """
        if not ylabel:
            ylabel = self.attributes
        if not title:
            title = self.host
        plot_handle = pl.figure()
        reader = csv.reader(results.splitlines())
        data = []
        for row in reader:
            data.append(row)
        data = np.array(data)
        pl.plot((data[1:, 0].astype(float) - data[1, 0].astype(float)) * 1e-3, data[1:, 1:].astype(float) * yscale)
        pl.xlabel("time [s]")
        pl.ylabel(ylabel)
        pl.title(title)
        return plot_handle

    @staticmethod
    def _is_windows():
        return os.name == 'nt'

    def __del__(self):
        self.jmxtool.close(force=True)
