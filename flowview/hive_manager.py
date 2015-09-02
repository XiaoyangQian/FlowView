# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)
from flowview.shell_executor import ShellExecutor, ShellException
from flowview import utils
import logging

logger = logging.getLogger(__name__)

class HiveManager(object):

    def __init__(self,db,table):
        """
        :param db:
        :param table:
        :return:
        """
        self.database = db
        self.table = table
        self.shell_exec = ShellExecutor()

    def pull_hive_ts(self,partition,hive_hive_ts_path):
        """
        Pulls hive timestamp from the given list of partitions
        and write results into the corresponding directory under FlowView database
        :param partition: partition in the format of 'year=YYYY/month=MM/day=DD/hour=HH'
        :return:
        """
        ptn_year,ptn_month,ptn_day,ptn_hour = utils.split_ptn(partition)

        pull_cmd = '''
        hive -e "use %s;
        insert overwrite directory '%s/%s/%s/%s/%s'
        select event_id, hive_timestamp from %s
        where year = %s and month = %s and day = %s and hour = %s;"''' \
                   %(self.database,hive_hive_ts_path,ptn_year,ptn_month,ptn_day,ptn_hour,
          self.table, ptn_year, ptn_month,ptn_day,ptn_hour)

        try:
            self.shell_exec.safe_execute(pull_cmd,splitcmd=False,as_shell=True)
        except Exception:
            logger.error("Error pulling data from %s hive table and writing to flowview database" %self.database)

        logger.info("Wrote hive timestamp data into hive table flowview.db/%s_hive_ts" %self.table)

    def get_new_ptns(self,last_ptn):
        """
        Retrieves partitions in Hive whose timestamps have not been retrieved.
        :param lastptn: Last processed partition in the format of 'year=YYYY/month=MM/day=DD/hour=HH'
        :return:  a list of partitions to process in the format of 'year=YYYY/month=MM/day=DD/hour=HH'
        """
        try:
            ptn_cmd = '''
            hive -e "use %s;
            show partitions %s;"
            ''' %(self.database,self.table)
            all_ptns = self.shell_exec.safe_execute(ptn_cmd,splitcmd=False,as_shell=True).output.split()
        except Exception:
            raise
        # If last partition is None, then the current load is the first load.
        # Process all existing partitions
        try:
             if last_ptn is None:
                 new_ptns = all_ptns
             else:
                 lastindex = all_ptns.index(last_ptn)
                 # Omit the very last ptn since Thrive/ETL pipeline may still be writing to it
                 new_ptns = all_ptns[lastindex + 1: -1]
        except ValueError:
             logger.error("Last processed partition %s not found in table %s"
                          %(last_ptn,self.table))
             raise
        logger.info("Retrieved processed partition %s" % last_ptn)
        logger.info("Pending partitions %s" %new_ptns)

        return new_ptns

    def create_hive_ts_ptn(self,partition,hive_hive_ts_path):
        """
        Creates hive partition
        :param partition: Partition to create in the format of 'year=YYYY/month=MM/day=DD/hour=HH'
        :return:
        """
        ptn_year,ptn_month,ptn_day,ptn_hour = utils.split_ptn(partition)

        create_ptn_cmd = '''
        hive -e "use flowview;
        alter table %s_hive add partition (year = %s, month = %s, day = %s, hour = %s)
        location '%s/%s/%s/%s/%s'";
        ''' %(self.table, ptn_year,ptn_month,ptn_day,ptn_hour,
              hive_hive_ts_path, ptn_year,ptn_month,ptn_day,ptn_hour)

        try:
            self.shell_exec.safe_execute(create_ptn_cmd,splitcmd=False,as_shell=True)
        except ShellException:
            logger.error("Error in creating hive table to store hive timestamp")
            raise

        logger.info("Created hive partition year = %s, month = %s, day = %s, hour = %s for %s"
                    %(ptn_year,ptn_month,ptn_day,ptn_hour,self.table))

    def count_hdfs_ptn_rows(self,ptn):
        (ptn_year,ptn_month,ptn_day,ptn_hour) = ptn.split("/")
        count_cmd = '''
        hive -e "use flowview;
        select count (*) from %s_hdfs
        where year = %s and month = %s and day = %s and hour = %s;"
        '''%(self.table,ptn_year,ptn_month,ptn_day,ptn_hour)

        try:
            row_count = self.shell_exec.safe_execute(count_cmd,splitcmd=False,as_shell=True).output
            return row_count
        except ShellException:
            logger.error("Error getting row counts for hdfs_ts partition %s" %ptn)
            raise

    def count_hive_ptn_rows(self,ptn):
        (ptn_year,ptn_month,ptn_day,ptn_hour) = ptn.split("/")
        ptn_hour_latency = int(ptn_hour) + 1
        where_clause = "%s_hdfs.year = %s and %s_hdfs.month = %s and %s_hdfs.day = %s and %s_hdfs.hour = %s " \
                       "and %s_hive.year = %s and %s_hive.month = %s and %s_hive.day = %s and %s_hive.hour = %s " \
                       "or %s_hive.hour = %s" \
                       %(self.table,ptn_year,self.table,ptn_month,self.table, ptn_day, self.table, ptn_hour,
                         self.table,ptn_year,self.table,ptn_month,self.table, ptn_day, self.table, ptn_hour,
                         self.table,ptn_hour_latency)

        count_cmd = '''
        hive -e "use flowview;
        select count (*) from %s_hdfs join %s_hive on (%s_hdfs.event_id = %s_hive.event_id)
        where %s;"
        ''' %(self.table,self.table,self.table,self.table,where_clause)

        try:
            matching_row_cnt = self.shell_exec.safe_execute(count_cmd,splitcmd=False,as_shell=True).output
            return matching_row_cnt
        except ShellException:
            logger.error("Error getting row counts")
            raise

    def load_ptn_transmitted_ratio(self,ptn):
        try:
            hdfs_ptn_row_cnt = self.count_hdfs_ptn_rows(ptn)
            hive_ptn_row_cnt = self.count_hive_ptn_rows(ptn)
            return (int(hive_ptn_row_cnt)/int(hdfs_ptn_row_cnt))
        except ShellException:
            logger.error("Error retrieving row counts")
            raise

    def purge(self):
        """
        Purges all existing hive table for the dataset.
        :return:
        """
        purge_cmd = '''
        hive -e "use flowview;
        drop table %s_hive;
        drop table %s_hdfs;"
        ''' %(self.table,self.table)

        try:
            self.shell_exec.safe_execute(purge_cmd,splitcmd=False,as_shell=True)
            logger.info("Purged hive tables")
        except ShellException:
            logger.error("Purging hive tables failed")
            raise