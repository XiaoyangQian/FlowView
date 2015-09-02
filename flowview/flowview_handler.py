# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)

from datetime import datetime
from flowview.metadata_manager import MetadataManager
from flowview.hive_manager import HiveManager
from flowview.hdfs_manager import hdfsManager
from flowview.shell_executor import ShellExecutor
from ConfigParser import SafeConfigParser

class FlowviewHandlerException(Exception):
    pass

class FlowviewHandler(object):
    """
    Base handler class for cleanup, setup, and load actions.
    The base class contains common functionality such as
    MetadataManager, HdfsManager, loadts, ShellExecutor, etc.
    """

    def __init__(self,config_file=None):
        """
        :param topic: Dataset's Trinity topic name
        :param db: Dataset's Thrive database name in Hive
        :table: Dataset's Thrive table name in Hive
        :return:
        """
        self.parser = SafeConfigParser()
        self.parser.read(config_file)

        self.topic = self.get_config("topic_name")
        self.database = self.get_config("database_name")
        self.table = self.get_config("table_name")
        self.connection_info = self.get_config("connection_info")
        self.metadata_mgr = MetadataManager(self.connection_info,self.table,self.topic)
        self.hdfs_mgr = hdfsManager(self.topic)
        self.hive_mng = HiveManager(self.database,self.table)
        self.shell_exec = ShellExecutor()
        self.loadts = datetime.now()

        self.hdfs_topic = "idea-flowview"

    def get_config(self,config):
        """
        Returns value of requested "config" using ConfigParser
        :param config:
        :return: value of configuration parameter "config"
        """
        return self.parser.get("main", config)