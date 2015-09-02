import re

def split_ptn(partition):
    """
    Transfers partition format to tuple of date objects
    :param partition: input partition format: year=YYYY/month=MM/day=DD/hour=HH
    :return: (YYYY, MM, DD, HH)
    """
    ptn_list = partition.split("/")
    ptn_value = tuple (item.split("=")[1] for item in ptn_list)

    return ptn_value

def dir_to_ptn(dir):
    """
    Transfers directory timestamp format to partition date objects
    :param dir: input directory format: YYYY-MM-DD HH:MM
    :return: (YYYY, MM, DD, HH, MM)
    """
    pattern = "([0-9]{4})\-([0-9]{2})\-([0-9]{2}) ([0-9]{2})\:([0-9]{2})"
    [(ptn_year,ptn_month,ptn_day,ptn_hour,ptn_min)] = re.findall(pattern,dir)
    return (ptn_year,ptn_month,ptn_day,ptn_hour,ptn_min)

def iso_format(dto):
    """
    Formats datetime object to ISO8601 format "yyyy-mm-dd HH:MM:SS"
    :param dto: datetime object
    :return: String with ISO timestamp
    """
    return dto.strftime("%Y-%m-%d %H:%M:%S")

