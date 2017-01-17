import os
import pexpect


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
        self.jmxtool = pexpect.spawn(command)

    def get_output(self):
        # Get all output
        result = ""
        try:
            for n in range(1, 10):
                result = result + self.jmxtool.read_nonblocking(size=16777216, timeout=2)
        except:
            pass
            # print("Error reading from JmxTool, buffer too small?")
        return result

    def __del__(self):
        self.jmxtool.close(force=True)
