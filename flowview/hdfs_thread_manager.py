import Queue
import threading
import logging
import os
import errno
from flowview import utils
from flowview.shell_executor import ShellExecutor, ShellException
from flowview.hdfs_manager import hdfsManager
logger = logging.getLogger(__name__)

exitFlag = 0

class HDFS_ThreadManager(threading.Thread):

    def __init__(self,name,workQueue,topic,table,ptn_list,local_hdfs_ts_path):
        """
        Initialization method for HDFS_TheadManager.
        Leverages Python multithreading library to accelerate
        HDFS file reading process.
        :param name: Name of the thread. E.g: Thread#
        :param workQueue: Queue storing directories to read from
        :param topic: Dataset's Trinity topic name
        :param table: Dataset's Thrive table name in Hive
        :param dir_list: A set list stores all of the partitions created
        :return:
        """
        threading.Thread.__init__(self)
        self.name = name
        self.topic = topic
        self.workQueue = workQueue
        self.table = table
        self.hdfsMng = hdfsManager(topic)
        self.ptn_list = ptn_list
        self.local_hdfs_ts_path = local_hdfs_ts_path

    def run(self):
        """
        Custmized run method that overrides the default multithreading run().
        :return:
        """
        logger.info("Starting %s" %self.name)
        self.process_data(self.name,self.workQueue,self.ptn_list,self.local_hdfs_ts_path)
        logger.info("Exiting %s" %self.name)

    def process_data(self,threadName,workQueue,ptn_list,local_hdfs_ts_path):
        """
        Supporting method for the run method.
        :param threadName:
        :param workQueue:
        :param ptn_list:
        :return:
        """
        while not exitFlag:
            queueLock = threading.Lock()
            # Lock the workQueue so that only one thread can read from it at a time
            queueLock.acquire()
            if not workQueue.empty():
                # Retrieve the directory to be processed
                dir_info = workQueue.get()
                ptn_year,ptn_month,ptn_day,ptn_hour,ptn_min = utils.dir_to_ptn(dir_info[0])
                # Add the processed directory timestamp into the directory list
                ptn_list.add("%s/%s/%s/%s" %(ptn_year,ptn_month,ptn_day,ptn_hour))
                # Localpath to temporarily store the timestamp files
                local_ptn_path = "%s/%s/%s/%s/%s" \
                            %(local_hdfs_ts_path,ptn_year,ptn_month,ptn_day,ptn_hour)
                filepath = ("%s/hdfs_ts.txt" %local_ptn_path)
                # Try making local file based on timestamp. Avoid error caused by race condition by ignoring
                # error caused by directory already exist.
                try:
                    os.makedirs(local_ptn_path)
                except OSError as Exception:
                    if Exception.errno != errno.EEXIST:
                        raise
                with open (filepath,"a+") as outfile:
                    self.hdfsMng.retrieve_hdfs_ts(dir_info,outfile)
            queueLock.release()


def hdfs_thread_execute(topic,table,hdfs_pending,ptn_list,local_hdfs_ts_path,hive_hdfs_ts_path):
    """
    Main hdfs thread executor.
    :param topic: Dataset's Trinity topic name
    :param table: Dataset's Thrive table name in Hive
    :param hdfs_pending: HDFS directories pending processing.
                         Expected format is a list of the following
                         (2015-08-19 10:12, /data/ds_ctg/trinity/thrive_test/d_20150819-1710)
    :return: The latest processed HDFS directory timestmap (e.g. 2015-08-19 10:12) after the current load
    """
    hdfs_mng = hdfsManager(topic)
    hdfs_new_last_dir = hdfs_pending[-1][0]
    threads = []
    workQueue = Queue.Queue()

    # remove previous local file, if it exists
    rmcmd = "rm -r -f %s" %local_hdfs_ts_path
    ShellExecutor.safe_execute(rmcmd)
    logger.info("Removed local file containing timestamps from previous load")

    threadNum = 11
    # Lock the hdfs_pending list so that only one thread can access it
    queueLock = threading.Lock()
    queueLock.acquire()
    for dir in hdfs_pending:
        workQueue.put(dir)
    queueLock.release()

    for t in range (1,threadNum):
        thread = HDFS_ThreadManager("thread%s"%t,workQueue,topic,table,ptn_list,local_hdfs_ts_path)
        thread.start()
        threads.append(thread)

    while not workQueue.empty():
        pass

    global exitFlag
    exitFlag = 1

    logger.info("Retrieved server & hdfs timestamp info of %s" %topic)

    # Only exit the main thread after all threads finish
    for t in threads:
        t.join()

    # For all directories processed in the current load,
    # (1) transfer the local files that stores the messages' timestamps
    #     to the proper Hive warehouse location
    # (2) create a partition that points toward that location
    for ptn in ptn_list:
        local_dir_path = "%s/%s/hdfs_ts.txt" %(local_hdfs_ts_path,ptn)
        hdfs_tgt_path = "%s/%s" %(hive_hdfs_ts_path,ptn)

        hdfs_mng.makedir(hdfs_tgt_path)
        hdfs_mng.force_putfile(local_dir_path,hdfs_tgt_path)
        hdfs_mng.create_hdfs_ts_ptn(ptn,table,hive_hdfs_ts_path)

    logger.info("Copied hdfs & server timestamp from local to hive warehouse")
    logger.info("Created hive partition for hdfs & server timestamp")
    logger.info("Exiting main thread")

    return hdfs_new_last_dir