# Data Pipeline - Utility Functions Module
# Created by: Gustav Christensen
# Date: December 2025
# Description: Common utility functions including timeout decorator for long-running operations.

import functools
from threading import Thread




def timeout(seconds_before_timeout):
    """
    Docstring: Timeout decorator/wrapper function. The timeout is specified when applying it to a function, e.g. @timeout(600) — the number is in seconds. 

    """

    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [
                Exception(
                    "function [%s] timeout [%s seconds] exceeded!"
                    % (func.__name__, seconds_before_timeout)
                )
            ]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e

            t = Thread(target=newFunc, daemon=True)
            try:
                t.start()
                t.join(seconds_before_timeout)
            except Exception as e:
                print("error starting thread")
                raise e
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret

        return wrapper

    return deco