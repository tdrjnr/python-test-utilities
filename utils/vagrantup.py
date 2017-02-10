import vagrant
from cd import cd


class VagrantUp:
    """
    Context manager for Vagrant VMs
    Will bring up the vagrant machines on entry and tear down on exit
    """

    def __init__(self, script_dir):
        """
        :param script_dir: directory in which your vagrant script resides
        """
        self.script_dir = script_dir

    def __enter__(self):
        if self.script_dir:
            with cd(self.script_dir):
                v = vagrant.Vagrant(quiet_stdout=False)
                v.up()
                print("...virtual cluster is up.")

    def __exit__(self, etype, value, traceback):
        if self.script_dir:
            with cd(self.script_dir):
                v = vagrant.Vagrant(quiet_stdout=False)
                v.halt()
