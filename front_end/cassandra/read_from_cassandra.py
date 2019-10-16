from cassandra.cluster import Cluster
from datetime import datetime
import os

cluster = Cluster([os.environ['CASSANDRA_DNS']])
session = cluster.connect(os.environ['CASSANDRA_KEYSPACE'])

def get_all_tags_from_cassandra(db=os.environ['CASSANDRA_TABLE']):
    """Extracts all Tags from the Cassandra keyspace"""
    tags_raw = list(session.execute('SELECT DISTINCT tag from {}'.format(db)))
    return [row.tag for row in tags_raw]


def keys_dict_cassandra(all_tags, db=os.environ['CASSANDRA_TABLE']):
    keys_dict = {}
    for tag in all_tags:
        tag_list = list(session.execute("SELECT keyword from {0} WHERE tag = '{1}'".format(db, tag)))
        tag_list = [row.keyword for row in tag_list]
        keys_dict[tag] = tag_list
    return keys_dict


def read_one_from_cassandra(tag, keyword, db=os.environ['CASSANDRA_TABLE']):
    dates_row = list(session.execute(
        "SELECT dates FROM {0} WHERE tag ='{1}' AND keyword = '{2}'".format(db, tag, keyword)))
    dates = dates_row[0].dates
    dates_formatted = ['{:%Y-%m}'.format(datetime.strptime(date, '%Y-%m-%d %H:%M:%S%z')) for date in dates]
    return dates_formatted

def datetime_x_y(dates_with_repetitions):
    """Give sorted datetimes and associated counts of posts"""
    dates_counter = dict(Counter(dates_with_repetitions))
    dates_sorted = sorted(list(set(dates_with_repetitions)))
    counts = [dates_counter[date] for date in dates_sorted]
    return dates_sorted, counts
