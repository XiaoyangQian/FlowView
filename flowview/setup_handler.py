# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)
from flowview.flowview_handler import FlowviewHandler
from flowview.metadata_manager import MetadataException
from flowview.shell_executor import ShellException
import logging

logger = logging.getLogger(__name__)

class SetupHandler(FlowviewHandler):
    """
    Handler for setup actions
    """
    def setup_metadata(self):
        """
        Generates and inserts setup phase metadata into FlowView SQL database
        HDFS and Hive last directory both default set as None
        :param None:
        :return: None
        """
        current_ts = self.loadts.strftime("%Y-%m-%d %H:%M:%S")
        load_metadata = {"topic_name":self.topic,
                         "database_name":self.database,
                         "table_name":self.table,
                         "load_start_time":current_ts,
                         "load_end_time":current_ts}
        self.metadata_mgr.insert(load_metadata,"load")

        self.metadata_mgr.close()

        logger.info("Finished setting up metadata")


    def set_up_hdfs_ts_table(self):
        """
        Sets up Hive table schema for loading message HDFS & Server timestamp
        :param None:
        :return: None
        """
        hdfs_ts_cmd = '''
        hive -e "use flowview;
        drop table if exists %s_hdfs;
        create table %s_hdfs (event_id string, server_timestamp timestamp, hdfs_timestamp timestamp)
        partitioned by (year int, month int, day int, hour int)
        row format delimited
        fields terminated by '\001';"
        ''' %(self.table,self.table)

        try:
            self.shell_exec.safe_execute(hdfs_ts_cmd,splitcmd=False,as_shell=True)
        except ShellException:
            logger.error("Error in creating %s server and hdfs timestamp table in Hive" %self.table)
            raise
        logger.info("Created %s server and hdfs timestamp table in Hive" %self.table)


    def set_up_hive_ts_table(self):
        """
        Sets up Hive table schema for loading message Hive timestamp
        :param None
        :return: None
        """
        hive_ts_cmd = '''
        hive -e "use flowview;
        drop table if exists %s_hive;
        create table %s_hive (event_id string, hive_timestamp timestamp)
        partitioned by (year int, month int, day int, hour int)
        row format delimited
        fields terminated by '\u0001'
        lines terminated by '\n';"
        ''' %(self.table,self.table)

        try:
            self.shell_exec.safe_execute(hive_ts_cmd,splitcmd=False,as_shell=True)
        except ShellException:
            logger.error("Error in creating %s hive timestamp table in Hive" %self.table)
            raise
        logger.info("Created %s hive timestamp table in Hive" %self.table)


    def execute(self):
        """
        Top level method for setup_handler, manages the setup workflow
        :return:
        """
        # Set up metadata on FlowView metadata database on SQL
        try:
            self.setup_metadata()
        except ShellException:
            logger.error("Error setting up metadata")
            raise

        # Set up Hive table for messages' Server & HDFS timestamps
        try:
            self.set_up_hdfs_ts_table()
        except ShellException:
            logger.error("Error setting up server & hdfs timestamp Hive table")
            raise

        # Set up Hive table for messages' Hive timestamps
        try:
            self.set_up_hive_ts_table()
        except ShellException:
            logger.error("Error setting up hive timestamp Hive table")
            raise

        logger.info("Successfully completed setup phase")



