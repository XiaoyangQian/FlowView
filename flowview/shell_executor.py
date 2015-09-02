# Author Rohan Kekatpure
# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)

import subprocess as sp


class ShellException(Exception):
    pass


class ShellResult(object):
    def __init__(self, retcode, output_iterator, error_iterator):
        self.retcode = retcode
        self.output = output_iterator
        self.error = error_iterator

    def __str__(self):
        return "retcode = {}\noutput = {}\nerror = {}".format(
            self.retcode, self.output, self.error)

    def __repr__(self):
        return self.__str__()


class ShellExecutor(object):
    """
    An anstraction over bash shell command layer. This class facilitates executing
    Shell commands needed by all classes in thrive core. There can be no logging in
    class because this class is used to set up the logger itself.
    """

    @staticmethod
    def execute(cmd_string, verbose=False, splitcmd=True, as_shell=False):
        """
        Executes command string
        :param cmd_string: Command string with arguments separated by spaces
        :return: Return code and an iterator object for output
        """

        if splitcmd:
            cmd = cmd_string.split(" ")
        else:
            cmd = cmd_string

        if verbose:
            print "[ShellExecutor::execute] %s" % cmd

        try:
            result = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=as_shell)
            output, error = result.communicate()
            retcode = result.returncode
            return ShellResult(retcode, output, error)
        except OSError:
            raise ShellException

    @staticmethod
    def safe_execute(cmd_string, **kwargs):
        """
        Executes command string and raises exception if return code is not 0
        :param cmd_string: Command string with arguments separated by spaces
        :return: output if return code is zero, raises exception otherwise
        """
        shell_result = ShellExecutor.execute(cmd_string, **kwargs)
        if shell_result.retcode != 0:
            raise ShellException
        else:
            return shell_result


