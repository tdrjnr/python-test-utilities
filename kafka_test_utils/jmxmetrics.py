import pexpect


class JmxMetrics:
    """
    Uses JMXTerm to get metrics from the Kafka broker
    """

    def __init__(self, connection):
        """
        :param connection: JMX address in form <hostname>:<port>
        """
        connection_timeout = 2

        self.jmxterm = pexpect.spawn("java -jar jmxterm.jar")
        self.jmxterm.expect_exact("$>")  # got prompt, can continue
        self.jmxterm.sendline("open " + connection)
        self.jmxterm.expect_exact("#Connection to " + connection + " is opened", connection_timeout)

    def get_metric(self, bean_type, bean_name, bean_value):
        request = "get -b kafka.server:type=" + bean_type + ",name=" + bean_name + " " + bean_value
        self.jmxterm.sendline(request)
        response_lines = [self.jmxterm.readline(), self.jmxterm.readline()]
        while response_lines and response_lines[-1] != "\r\n":
            try:
                response_lines.append(self.jmxterm.readline())
            except pexpect.exceptions.TIMEOUT:
                print("Response to metric value request timed out.")
                return response_lines

        return response_lines

    def __del__(self):
        self.jmxterm.sendline("quit")
        self.jmxterm.expect_exact("#bye")
