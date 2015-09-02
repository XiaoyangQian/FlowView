import logging
from flowview.metadata_manager import MetadataException
from flowview.shell_executor import ShellException
from flowview_handler import FlowviewHandler

logger = logging.getLogger(__name__)

class CleanupHandler(FlowviewHandler):
    """
    Handler fpr cleaning up Hive table, metadata, HDFS directories
    Local HDFS files will be cleaned up by the thread_manager
    """
    def execute(self):

        # cleans up Hive tables
        try:
            self.hive_mng.purge()
        except ShellException:
            logger.error("Purging hive tables failed")
            raise

        # cleans up SQL Metadata
        try:
            self.metadata_mgr.purge()
        except MetadataException:
            logger.error("Purging metadata failed")
            raise


