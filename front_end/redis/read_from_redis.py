import redis
import os
from datetime import datetime
from collections import Counter, defaultdict


def redis_connect(host=os.environ["REDIS_DNS"], port=os.environ["REDIS_PORT"], db=os.environ["REDIS_DB"]):
    return redis.Redis(host=host, port=port, db=db)


def redis_get_all_compound_keys(redis):
    """Extracts all (tag, keyword) pairs from redis database."""
    keys = defaultdict(list)
    for key in redis.scan_iter("*"):
    for key in redis.scan_iter("*"):
        key_pair = tuple(key.decode("utf-8").split(':'))
        if len(key_pair) == 2:  # omit keys corresponding to schema
            keys[key_pair[0]].append(key_pair[1])
    return keys


def read_one_from_redis(tag, keyword):
    """Given a tag and a keyword, extracts the corresponding value (time series). Converts to months"""
    redis_hash = str(tag) + ":" + str(keyword)
    read = redis_connect().hgetall(redis_hash)
    dates_extract = list(read.values())[0].decode("utf-8")  # extract bytecode
    dates_only = dates_extract[13:-1]  # Removes WrappedArray(...) bytecode
    dates_split = [s.strip() for s in dates_only.split(',')]
    dates_formatted = ['{:%Y-%m}'.format(datetime.strptime(date, '%Y-%m-%d')) for date in dates_split]
    return dates_formatted


def datetime_x_y(dates_with_repetitions):
    """Give sorted datetimes and associated counts of posts"""
    dates_counter = dict(Counter(dates_with_repetitions))
    dates_sorted = sorted(list(set(dates_with_repetitions)))
    counts = [dates_counter[date] for date in dates_sorted]
    return dates_sorted, counts
