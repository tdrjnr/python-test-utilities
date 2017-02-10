import os
import pexpect
import matplotlib.pyplot as pl
import csv
import numpy as np


class JmxTool:
    def __init__(self, build_dir, host, topic="system_test_topic", metrics="broker"):
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

        bash_script = "bash " + os.path.join(build_dir, "system_tests", "kafka_2.11-0.9.0.1", "bin",
                                             "kafka-run-class.sh")

        command = bash_script + (" kafka.tools.JmxTool "
                                 "--object-name " + mbean1)
        if mbean2:
            command = command + "--object-name " + mbean2

        command = (command + "--jmx-url "
                             "service:jmx:rmi:///jndi/rmi://" + host +
                   "/jmxrmi "
                   "--attributes " + attributes +
                   "--reporting-interval 2000")
        self.host = host
        self.attributes = attributes.strip()
        self.jmxtool = pexpect.spawn(command)

    def get_output(self, plot=True):
        # Get all output
        results = ""
        try:
            for n in range(1, 10):
                results = results + self.jmxtool.read_nonblocking(size=16777216, timeout=2)
        except:
            pass
            # print("Error reading from JmxTool, buffer too small?")
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

    def __del__(self):
        self.jmxtool.close(force=True)
