import os
import subprocess


class KafkaSubprocess:
    def __init__(self, command, working_dir=None, env=None):
        """Changes into a specified directory, if provided, and executes a command.
        Restores the old directory afterwards.
        Args:
          command:        The command to run, in the form of sys.argv.
          working_dir:    The directory to change into.
          env:            Dictionary with environment to pass to the subprocess.
        Returns:
          An object that represents outcome of the executed process. It has the
          following attributes:
            terminated_by_signal   True if the child process has been terminated
                                   by a signal.
            signal                 Signal that terminated the child process.
            exited                 True if the child process exited normally.
            exit_code              The code with which the child process exited.
            output                 Child process's stdout and stderr output
                                   combined in a string.
        """

        # The subprocess module is the preferable way of running programs
        # since it is available and behaves consistently on all platforms,
        # including Windows. Note it is only available starting in python 2.4.

        # Discard stdout by using os.devnull. Writing stdout to file causes a big performance
        # problem whilst reading the nexus file. Using subprocess.PIPE is horrible it uses a very
        # small buffer and the process will simply hang when the buffer is full.
        self.discard_stdout = open(os.devnull, 'w')
        self.p = subprocess.Popen(command,
                                  stderr=subprocess.STDOUT, stdout=self.discard_stdout,
                                  cwd=working_dir, universal_newlines=True, env=env)

    def wait(self):
        """
        Blocks until the process has exited
        """
        # communicate returns a tuple with the file object for the child's
        # output.
        self.output = self.p.communicate()[0]
        _return_code = self.p.returncode

        if _return_code < 0:
            self.terminated_by_signal = True
            self.exited = False
            self.signal = -_return_code
        else:
            self.terminated_by_signal = False
            self.exited = True
        self.discard_stdout.close()
