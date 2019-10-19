from datetime import datetime, timedelta
from functools import wraps
import numpy as np
import pandas as pd
import time

# Took some code from: https://github.com/camisatx/pySecMaster/blob/master/pySecMaster/download.py

def IMF_rate_limit(rate=10, period_sec=5, threads=1):
    """
    A decorator that limits the rate at which a function is run. If the function
    is run over that rate, a forced sleep will occur. The main purpose of this
    is to make sure an API is not overloaded with requests. For Quandl, the
    default API limit is 2,000 calls in a 10 minute time frame. If multiple
    threads are using the API concurrently, make sure to increase the threads 
    variable to the number of threads being used.
    :param rate: Integer of the number of items that are downloaded
    :param period_sec: Integer of the period (seconds) that the rate occurs in
    :param threads: Integer of the threads that will be running concurrently
    """

    optimal_rate = float((rate / period_sec) / threads)
    min_interval = 1.0 / optimal_rate

    def rate_decorator(func):
        last_check = [0.0]

        @wraps(func)
        def rate_limit_func(*args, **kargs):
            elapsed = time.time() - last_check[0]
            time_to_wait = min_interval - elapsed
            if time_to_wait > 0:
                time.sleep(time_to_wait)
                # print('Sleeping for %0.2f seconds' % int(time_to_wait))
            ret = func(*args, **kargs)
            last_check[0] = time.time()

            return ret
        return rate_limit_func
    return rate_decorator
