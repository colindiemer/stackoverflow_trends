from cassandra.cluster import Cluster
from collections import defaultdict
from datetime import datetime
import os

cluster = Cluster([os.environ['CASSANDRA_DNS']])
session = cluster.connect('dev')

def get_all_tags_from_cassandra(db='so_test'):
    tags_raw = list(session.execute('SELECT DISTINCT tag from {}'.format(db)))
    return [row.tag for row in tags_raw]

def keys_dict_cassandra(db='so_test'):
    all_raw_pairs = list(session.execute('SELECT tag, keyword from {}'.format(db)))
    keys_dict = defaultdict(list)
    for row in all_raw_pairs:
        keys_dict[row.tag].append(row.keyword)
    return keys_dict

def read_dates_from_cassandra(tag, keyword, db='so_test'):
    dates_row = list(session.execute(
        "SELECT dates FROM {0} WHERE tag ='{1}' AND keyword = '{2}'".format(db, tag, keyword)))
    dates = dates_row[0].dates
    dates_formatted = ['{:%Y-%m}'.format(datetime.strptime(date, '%Y-%m-%d %H:%M:%S%z')) for date in dates]
    return dates_formatted

print(get_all_tags_from_cassandra())