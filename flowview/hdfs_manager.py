# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)

from shell_executor import ShellExecutor, ShellException
from datetime import datetime
import logging
import re

logger = logging.getLogger(__name__)

class hdfsManager(object):
    """
    Manager class for HDFS directory operations.
    """
    def __init__(self,topic):
        self.topic = topic
        self.shell_exec = ShellExecutor()

    def makedir(self, dir_path):
        """
        Makes an HDFS directory at dirpath
        :param dir_path: HDFS path
        :return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -mkdir -p %s" % dir_path)
            logger.info("Created %s" % dir_path)
        except ShellException:
            logger.error("HDFS makedir failed. %s" % dir_path)
            raise

    def rmdir(self, dir_path):
        """
        Removes HDFS directory at dir_path
        :param dir_path: HDFS path
        :return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -rm -r %s" % dir_path)
            logger.info("Deleted %s" % dir_path)
        except ShellException:
            logger.error("HDFS rmdir failed. %s" % dir_path)
            raise

    def putfile(self, local_path, hdfs_path):
        """
        Copies a resource from local path to HDFS path
        :param localpath: Path to resource on local fileystem
        :param hdfspath: HDFS destination path
        :return: None
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -put %s %s" % (local_path, hdfs_path))
            logger.info("Put %s to HDFS path %s" % (local_path, hdfs_path))
        except ShellException:
            logger.error("HDFS putfile failed. %s %s" % (local_path, hdfs_path))
            raise

    def force_putfile(self,local_path,hdfs_path):
        """

        :param local_path:
        :param hdfs_path:
        :return:
        """
        try:
            self.shell_exec.safe_execute("hadoop fs -put -f %s %s" %(local_path,hdfs_path))
            logger.info("Force put %s to HDFS path %s" % (local_path, hdfs_path))
        except ShellException:
            logger.error("HDFS force putfile failed. %s %s" % (local_path, hdfs_path))
            raise

    def get_start_dir(self,last_dir,start_dir):
        """

        :param last_dir:
        :param start_dir:
        :return:
        """
        if start_dir is None:
            return last_dir
        else:
            last_dir_int = re.sub("[^0-9]", "",str(last_dir))
            start_dir_int = re.sub("[^0-9]", "",str(start_dir))
            if (last_dir_int < start_dir_int):
                return start_dir
            else:
                return last_dir

    def get_new_dirs(self,last_dir,start_dir,hdfs_path):
        """
        :param last_dir: Last processed hdfs directory
        :return: A list of hdfs directories pending processing.
                Expected format is a list of the following:
                (2015-08-19 10:12, /data/ds_ctg/trinity/thrive_test/d_20150819-1710)
        """
        # path and command to trinity topic for the dataset
        cmd = "hadoop fs -ls %s" % hdfs_path
        result = self.shell_exec.safe_execute(cmd)
        output = result.output
        # stores snappy file list in output
        # sample output:
        # drwxr-xr-x   - sys_bio_ctgdq bio_hadoop_ds          0 2015-08-19 10:12 /data/ds_ctg/trinity/thrive_test/d_20150819-1710

        # Compile pattern containing
        # (1) hdfs processing timestamp e.g: 2015-08-19 10:12
        # (2) path leading to the snappy file e.g: /data/ds_ctg/trinity/thrive_test/d_20150819-1710
        dentry_pattern = ".*([0-9]{4}\-[0-9]{2}\-[0-9]{2} [0-9]{2}:[0-9]{2}) (.*)"
        all_dirs = re.findall(dentry_pattern, output)

        # Sort dirs according to date. Date is obtained from dirname
        # (e.g. 'd_20150311-1610') by retaining only the numeric parts of
        # the string and converting to int (e.g. 201503111610)
        all_dirs.sort(key=lambda  s: int(re.sub("[^0-9]", "", s[1])))

        proceed_dir = self.get_start_dir(last_dir,start_dir)
        try:
            # If last directory is None, indicating the current load is the first load,
            # process all directories found
            if proceed_dir is None:
                pending_dir_info = all_dirs
            # Else, index the last directories' position in all directories
            # Because dir_info contains both timestamp and path info in a list,
            # need to go into sublist to index proceed_dir
            else:
                lastindex = next((i for i, sublist in enumerate(all_dirs) if proceed_dir in sublist), -1)
                pending_dir_info = all_dirs[lastindex +1: -1]
        except ValueError:
            logger.error("Last processed directory %s not found in topic location %s" %(proceed_dir,hdfs_path))
            raise

        pending_dir = []
        for item in pending_dir_info:
            pending_dir.append(item[0])

        logger.info("Retrived processed directory %s" %proceed_dir)
        logger.info("Pending directories  %s" %pending_dir)

        return pending_dir_info


    def retrieve_hdfs_ts(self,dir_info,outfile):
        """
        Retrieve server & hdfs timestamp. Note that this method is called in a loop in hdfs_thread_manager
        :param dir_info: HDFS timestamp and path to message fetched from snappy file header
        :param outfile: File to write the line of output to
        :return:
        """
        try:
            hdfs_ts, path = dir_info
            cmd = "hadoop fs -text %s/*" %path
            result = self.shell_exec.safe_execute(cmd)
            # Fetches individual messages
            msg = result.output
            # Compile regular expression pattern to fetch event_id and server_timestamp
            pattern = re.compile('.*"event_id":"(.*?)".*?"server_timestamp":([0-9]+)')
            # Store parsed data
            event_data = re.findall(pattern,msg)

            # Loop through individual message event data
            for item in event_data:
                event_id,server_unix_ts = item
                # Convert UNIX timestamp in milliseconds to standard timestamp
                server_ts = datetime.fromtimestamp(int(server_unix_ts)/1000).strftime("%Y-%m-%d %H:%M:%S")
                # Create output. Add seconds to hdfs_ts so that it is recognizable by Hive timestamp format
                output = (event_id,server_ts,hdfs_ts+":00\n")
                # Separate fields with ControlA
                ctrl_A = '\x01'
                outfile.write(ctrl_A.join(output))

        except Exception:
            raise

    def create_hdfs_ts_ptn(self,partition,table,hive_hdfs_ts_path):
        """
        Adding on partition to the server & hdfs timestamp Hive table
        :param partition: The partition to be created. Expected format: YYYY/MM/DD/HH
        :param table:
        :return:
        """
        ptn_year,ptn_month,ptn_day,ptn_hour = partition.split("/")

        create_ptn_cmd = '''
        hive -e "use flowview;
        alter table %s_hdfs add if not exists partition (year = %s, month = %s, day = %s, hour = %s)
        location '%s/%s/%s/%s/%s'";
        ''' %(table, ptn_year,ptn_month,ptn_day,ptn_hour,
              hive_hdfs_ts_path, ptn_year,ptn_month,ptn_day,ptn_hour)
        try:
            self.shell_exec.safe_execute(create_ptn_cmd,splitcmd=False,as_shell=True)
        except ShellException:
            logger.error("Error in creating hive table to store server and hdfs timestamp")
            raise