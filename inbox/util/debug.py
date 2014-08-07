"""Utilities for debugging failures in development/staging."""
from functools import wraps
import pdb
from inbox.log import log_uncaught_errors
from pyinstrument import Profiler
import cProfile, pstats, gc, time
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


def cprofile(fn):
    def wrapper(*args, **kw):
        elapsed, stat_loader, result = _profile('cmessage.txt', fn, *args, **kw)
        stats = stat_loader()
        stats.sort_stats('cumulative')
        stats.print_stats()
        # Uncomment this to see who's calling what
        # stats.print_callers()
        return result
    return wrapper


def _profile(filename, fn, *args, **kw):
    load_stats = lambda: pstats.Stats(filename)
    gc.collect()

    began = time.time()
    cProfile.runctx('result = fn(*args, **kw)', globals(), locals(),
                    filename=filename)
    ended = time.time()

    return ended - began, load_stats, locals()['result']


def attach_profiler():
    profiler = Profiler()
    profiler.start()

    def handle_signal(signum, frame):
        print profiler.output_text(color=True)
        # Work around an arguable bug in pyinstrument in which output gets
        # frozen after the first call to profiler.output_text()
        delattr(profiler, '_root_frame')

    signal.signal(signal.SIGTRAP, handle_signal)
