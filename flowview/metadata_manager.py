# Author Rohan Kekatpure
# Referenced from IDEA Thrive (https://github.intuit.com/idea/thrive)

import pyodbc
import logging

logger = logging.getLogger(__name__)

class MetadataException(Exception):
    pass


class MetadataManager(object):
    """
    Class for managing metadata for the FlowView setup and load process. This is
    really only a CRUD layer + business logic on top of the MySQL metadata
    database.

    Primary responsibilities of this class include supplying a metadata
    manager object capable of communicating with the metadata DB and updating
    the metadata after every load event.
    """
    def __init__(self, connection_info,table,topic):
        """
        Initializes the connection object by establishing connection with
        the metadata DB
        :param connection_info: parameters required for connection
        :return: None
        """
        self.connection_info = connection_info
        self.topic = topic
        self.table = table
        try:
            self.connection = pyodbc.connect(
                "DRIVER={%s};SERVER=%s;PORT=%s;UID=%s;PWD=%s;DB=%s"
                % (self.connection_info["md_dbtype"],
                   self.connection_info["md_dbhost"],
                   self.connection_info["md_dbport"],
                   self.connection_info["md_dbuser"],
                   self.connection_info["md_dbpass"],
                   self.connection_info["md_dbname"])
            )
        except pyodbc.Error, poe:
            errmsg = "Could not connect to database"
            logger.error(errmsg)
            raise MetadataException(errmsg)

    def insert(self,data,mdtype=None):
        if mdtype == "ratio":
            mdtable = "flowview_load_transmitted_ratio"
        elif mdtype == "load":
            mdtable = "flowview_load_metadata"
        else:
            errmsg = "Invalid metadata type: %s" % mdtype
            logger.error(errmsg)
            raise MetadataException(errmsg)

        columns = ",".join(data.keys())
        values = ",".join("'%s'" % v for v in data.values())

        insert_qry = "insert into %s (%s) values (%s);" \
                     % (mdtable, columns, values)

        try:
            self.execute(insert_qry)
        except pyodbc.IntegrityError, ie:
            errmsg = "Duplicate primary key insertion"
            logger.error(errmsg)
            raise MetadataException(errmsg)
        except pyodbc.Error, poe:
            errmsg = "Could not insert data"
            logger.error(errmsg)
            raise MetadataException(errmsg)

    def get_hdfs_lastdir(self):
        """
        Returns the last directory processed for "topic" by querying "table"
        :param topic: dataset being loaded
        :return: results
        """

        qry = '''
                 select last_load_hdfs_dir
                 from flowview_load_metadata
                 where topic_name = '%s'
                 order by load_end_time desc
                 limit 1;
              ''' % (self.topic)

        hdfs_last_ptn = self.execute_return(qry)[0][0]
        return hdfs_last_ptn

    def get_hive_lastptn(self):
        qry = '''
                 select last_load_hive_partition
                 from flowview_load_metadata
                 where topic_name = '%s'
                 order by load_end_time desc
                 limit 1;
              ''' % (self.topic)

        hive_last_partition = self.execute_return(qry)[0][0]
        return hive_last_partition


    def execute(self, qry):
        """
        Function for executing queries which dont return results.
        Sends SQL query "qry" to metadata database.
        :param qry: SQL query string
        :return: None
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(qry)
            self.connection.commit()
            cursor.close()
        except pyodbc.Error, poe:
            raise poe

    def execute_return(self, qry):
        """
        Function for executing queries which return results.
        Sends SQL query "qry" to metadata database.
        :param qry: SQL query string
        :return: None
        """
        try:
            cursor = self.connection.cursor()
            cursor.execute(qry)
            self.connection.commit()
            results = cursor.fetchall()
            cursor.close()
            return results
        except pyodbc.Error, poe:
            raise poe

    def get_dailyload(self,date):
        """
        Returns loads done in the current day.
        param: table topic, current date in the format of YYYMMDD
        return: results
        Should be triggered at the end of day
        """
        qry = '''
                 select *
                 from flowview_load_metadata
                 where topic_name = '%s' and
                 last_load_folder between
                 'd_%s-0000' and 'd_%s-2400'
                 order by load_end_time;
              ''' %(self.topic,date,date)

        dailyload = self.execute_return(qry)
        return dailyload

    def close(self):
        self.connection.close()

    def purge(self):
        """
        Purges the SQL metadata dataset 'flowview_load_metadata'
        :return:
        """
        try:
            for md_table in ("flowview_load_metadata","flowview_load_transmitted_ratio"):
                purge_setup_qry = "delete from %s where topic_name = '%s';" \
                                  % (md_table, self.topic)
                self.execute(purge_setup_qry)
            logger.info("Purged metadata tables")
        except pyodbc.Error:
            logger.error("Purge failed for dataset %s" % self.topic)
            raise