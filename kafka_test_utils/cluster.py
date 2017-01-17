import vagrant
from cd import cd


class Cluster:
    """
    Manage the virtual cluster
    """

    def __init__(self, repo_dir):
        self.repo_dir = repo_dir

    def __enter__(self):
        if self.repo_dir:
            with cd(self.repo_dir):
                v = vagrant.Vagrant(quiet_stdout=False)
                v.up()
                print("...virtual cluster is up.")

    def __exit__(self, etype, value, traceback):
        if self.repo_dir:
            with cd(self.repo_dir):
                v = vagrant.Vagrant(quiet_stdout=False)
                v.halt()
