"""Utilities for debugging failures in development/staging."""
from functools import wraps
import pdb
from inbox.log import log_uncaught_errors
from pyinstrument import Profiler
import cProfile, pstats, StringIO
import signal


def pause_on_exception(exception_type):
    """Decorator that catches exceptions of type exception_type, logs them, and
    drops into pdb. Useful for debugging occasional failures.

    Example
    -------
    >>> @pause_on_exception(ValueError)
    ... def bad_function():
    ...     # Do stuff
    ...     raise ValueError
    """
    def wrapper(func):
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_type:
                log_uncaught_errors()
                pdb.post_mortem()
        return wrapped
    return wrapper


def profile(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        profiler = Profiler()
        profiler.start()
        r = func(*args, **kwargs)
        profiler.stop()

        filename = 'message.out'
        with open(filename, 'a+') as f:
            f.write(profiler.output_text(color=True))
        return r
    return wrapper


def cprofile(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        pr = cProfile.Profile()

        pr.enable()
        r = func(*args, **kwargs)
        pr.disable()

        filename = 'cmessage.out'
        s = StringIO.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.dump_stats(filename)

        print s.getvalue()

        return r
    return wrapper


def attach_profiler():
    profiler = Profiler()
    profiler.start()

    def handle_signal(signum, frame):
        print profiler.output_text(color=True)
        # Work around an arguable bug in pyinstrument in which output gets
        # frozen after the first call to profiler.output_text()
        delattr(profiler, '_root_frame')

    signal.signal(signal.SIGTRAP, handle_signal)
