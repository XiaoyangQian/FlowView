# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)
from flowview.flowview_handler import FlowviewHandler
from flowview.hive_manager import HiveManager
from flowview.hdfs_thread_manager import hdfs_thread_execute
from flowview.metadata_manager import MetadataException
from flowview import utils
import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

class LoadHandler(FlowviewHandler):
    """
    Handler for the loading phase. Should be triggered on an hourly basis by job scheduler.
    """

    def __init__(self,config_file=None):
        """
        Initializes the FlowviewHandler superclass and instantiates manager classes
        needed to performing load-related actions
        :param topic: Dataset's Trinity topic name
        :param db: Dataset's Thrive database name in Hive
        :param table: Dataset's Thrive table name in Hive
        :return: None
        """
        super(LoadHandler,self).__init__(config_file)
        self.hive_mgr = HiveManager(self.topic,self.table)
        self.hdfs_dir_pending = None
        self.hdfs_ptn_list = set()
        self.hive_ptn_pending = None
        self.hdfs_new_last_dir = None
        self.hive_new_last_ptn = None
        self.hdfs_proceed = False
        self.hive_proceed = False
        logger.info("Starting load of %s %s" %(self.topic,self.table))

    def get_start_ptn(self,last_ptn, start_ptn):
        """

        :param last_dir:
        :param start_dir:
        :return:
        """
        if start_ptn is None:
            return last_ptn
        else:
            last_ptn_int = re.sub("[^0-9]", "",str(last_ptn))
            start_ptn_int = re.sub("[^0-9]", "",str(start_ptn))
            if (last_ptn_int < start_ptn_int):
                return start_ptn
            else:
                return last_ptn

    def get_hive_newptns(self):
        """
        Get unprocessed hive partitions.
        This method retrieves the last processed partition from
        metadata, leverages HiveManager to receive the pending
        partitions, and calculates the latest processed partition
        at the end of this load by taking the last partition
        from pending hive partition list.
        :param None:
        :return: list of hive partitions pending process
        """
        hive_old_lastptn = self.metadata_mgr.get_hive_lastptn()
        start_ptn = self.get_start_ptn(hive_old_lastptn, self.get_config("hive_start_ptn"))
        self.hive_ptn_pending = self.hive_mgr.get_new_ptns(start_ptn)
        self.hive_new_lastptn = self.hive_ptn_pending[-1] if self.hive_ptn_pending else hive_old_lastptn
        logger.info("Starting ptn = %s, ending ptn = %s"
                    %(start_ptn,self.hive_new_lastptn))

    def hive_to_proceed(self):
        """
        Determines if processing for hive partitions should proceed.
        :return: True if there exists partitions pending processing, False if none.
        """
        try:
            # retrieve Hive partitions pending processing
            self.get_hive_newptns()
            if not self.hive_ptn_pending:
                self.hive_proceed = False
            else:
                self.hive_proceed = True
        except Exception:
            logger.error("Error retrieving last processed hive partition")
            raise
        logger.info("Retrived hive partition pending processing %s" %self.hive_ptn_pending)

    def hdfs_to_proceed(self):
        """
        Determines if processing for hdfs directories should proceed.
        :return: True if there exists directories pending processing, False if none.
        """
        try:
            # Retrieve last processed hdfs directory
            hdfs_last_dir = self.metadata_mgr.get_hdfs_lastdir()
            # Calculate hdfs directories pending processing
            self.hdfs_dir_pending = self.hdfs_mgr.get_new_dirs(hdfs_last_dir,
                                                               self.get_config("start_dir"),
                                                               self.get_config("hdfs_path"))
            if not self.hdfs_dir_pending:
                self.hdfs_proceed = False
            else:
                self.hdfs_proceed = True
        except Exception:
            logger.error("Error retrieving last processed hdfs directories")
            raise

    def insert_ptn_ratio(self):

        ptn_list_sorted = sorted(self.hdfs_ptn_list,key=lambda  s: int(re.sub("[^0-9]", "", s)))

        try:
            for ptn in ptn_list_sorted:
                transmitted_ratio = self.hive_mng.load_ptn_transmitted_ratio(ptn)
                load_success_data = {
                        "topic_name": self.topic,
                        "database_name": self.database,
                        "table_name":self.table,
                        "load_start_time":utils.iso_format(self.loadts),
                        "load_end_time": utils.iso_format(datetime.now()),
                        "hdfs_partition":ptn,
                        "transmitted_ratio":transmitted_ratio
                }

                print load_success_data
                self.metadata_mgr.insert(load_success_data,"ratio")
        except Exception:
            logger.error("Error creating transmission ratio metadata")
            raise



    def execute(self):
        # TODO [comment section (2)] based on??
        """
        Top level method for LoadHandler; manages the load workflow.
        The method
        (1) decides if the current load should proceed
        (2) creates Hive table for server & HDFS timestamp partitioned by
        Year, Month, Day, Hour based on
        (3) creates hive table for hive timestamp partitioned by
        Year, Month, Day, Hour based on source Hive table partition
        :return: None
        """
        # Determines if there exist hdfs directories and hive partitions to process
        self.hdfs_to_proceed()
        self.hive_to_proceed()
        if not self.hive_proceed and not self.hdfs_proceed:
            logger.info("No partitions or directories to proceed. Ending load")
            return

        logger.info("Proceeding with load")

        if self.hdfs_proceed:
            logger.info("Proceeding with HDFS load")
            try:
                # Calculate the latest last processed hdfs directory after the current load
                # hdfs_thread_manager retrieves hdfs timestamps from pending directories,
                # write to local file system, copy files to hive warehouse,
                # create new partitions that point toward corresponding directories
                self.hdfs_new_last_dir = hdfs_thread_execute(self.topic,self.table,self.hdfs_dir_pending,
                                                             self.hdfs_ptn_list,
                                                             self.get_config("local_hdfs_ts_path"),
                                                             self.get_config("hive_hdfs_ts_path"))
            except Exception:
                logger.error("Error retrieving server and hdfs timestamp")
                raise
        else:
            logger.info("No new HDFS dir to process.")

        if self.hive_proceed:
            hive_hive_ts_path = self.get_config("hive_hive_ts_path")
            logger.info("Proceeding with Hive load")
            try:
                # create a FlowView partition for each Hive partition pending processing
                for partition in self.hive_ptn_pending:
                    # create partition
                    self.hive_mgr.create_hive_ts_ptn(partition,hive_hive_ts_path)
                    # retrieve hive timestamp data and write into the corresponding directory
                    self.hive_mgr.pull_hive_ts(partition,hive_hive_ts_path)
                # calculate the last processed partition after the current load
                self.hive_new_last_ptn = self.hive_ptn_pending[-1]
            except Exception:
                logger.error("Error creating hive partition")
                raise

            logger.info("Created hive partition for Hive timestamp")

        else:
            logger.info("No new Hive partition to process")

        try:
            # create metadata for current load
            load_metadata = {
                "topic_name": self.topic,
                "database_name": self.database,
                "table_name":self.table,
                "load_start_time":utils.iso_format(self.loadts),
                "load_end_time": utils.iso_format(datetime.now()),
                "last_load_hdfs_dir": self.hdfs_new_last_dir,
                "last_load_hive_partition": self.hive_new_last_ptn
            }
            logger.info("Created load_metadata %s" %load_metadata)
        except Exception:
            logger.error("Error creating metadata")
            raise
        logger.info("Created hive partition for hive timestamp")

        try:
            # insert metadata for current load into SQL metadata database
            self.metadata_mgr.insert(load_metadata,"load")
        except MetadataException:
                logger.error("Error inserting metadata %s" %load_metadata)
                raise

        try:
            self.insert_ptn_ratio()
            logger.info("Calculated load partition data transmission ratio")
        except Exception:
            logger.error("Error in calculating load partition data transmission ratio")
            raise

        logger.info("Load complete")