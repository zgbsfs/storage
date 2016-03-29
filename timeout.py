from functools import wraps
import errno
import os
import signal

class TimeoutError(Exception):
    pass

def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)
        
        def wrapper(*args, **kwargs):
	    g = func.func_globals
	    sentinel = object()

	    oldvalue = g.get('sp', sentinel)
            signal.signal(signal.SIGALRM, _handle_timeout)
	    print oldvalue
            signal.alarm(oldvalue)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result
        
        return wraps(func)(wrapper)
    
    return decorator
