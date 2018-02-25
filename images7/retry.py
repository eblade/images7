#!/usr/bin/env python3


from jsondb import Conflict
import time


class GiveUp(Exception):
    pass


def retry(max_retries=10, timeout=30):
    def wraps(func):
        def inner(*args, **kwargs):
            for i in range(max_retries):
                try:    
                    result = func(*args, **kwargs)
                except Conflict:
                    time.sleep(timeout)
                    continue
                else:
                    return result
            else:
                raise GiveUp 
        return inner
    return wraps