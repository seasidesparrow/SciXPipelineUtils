import ast
import json
import os
import sys
import types

import unidecode

from SciXPipelineUtils.exceptions import UnicodeHandlerError

#imports for setup_logging
import atexit
import inspect
import logging
import logging.handlers 
import queue
import socket
import threading
import time
from datetime import datetime, timezone
from logging import Formatter
        
from concurrent_log_handler import ConcurrentRotatingFileHandler
from dateutil import tz
        
try:  # python-json-logger >= 3.1
    from pythonjsonlogger.json import JsonFormatter as _BaseJsonFormatter
except ImportError:  # older python-json-logger
    from pythonjsonlogger.jsonlogger import JsonFormatter as _BaseJsonFormatter
#end setup_logging imports


def get_schema(app, schema_client, schema_name):
    """
    input:

    app: The relevant calling application (can be any class with a logger attirbute.)
    schema_client: Kafka SchemaRegistryClient
    schema_name: The name of the AVRO schema

    return:

    AVRO schema (str)
    """
    try:
        avro_schema = schema_client.get_latest_version(schema_name)
        app.logger.info("Found schema: {}".format(avro_schema.schema.schema_str))
    except Exception as e:
        avro_schema = None
        app.logger.warning("Could not retrieve avro schema with exception: {}".format(e))

    return avro_schema.schema.schema_str


def load_config(proj_home=None, extra_frames=0, app_name=None):
    """
    Loads configuration from config.py and also from local_config.py

    :parameter proj_home: - str, location of the home - we'll always try
        to load config files from there. If the location is empty,
        we'll inspect the caller and derive the location of its parent
        folder.
    :parameter extra_frames: - int, number of frames to look back; default
        is 2, which is good when the load_config() is called directly,
        but when called from inside classes, we need to add extra more
    :return: dictionary
    """
    conf = {}

    if proj_home is not None:
        proj_home = os.path.abspath(proj_home)
        if not os.path.exists(proj_home):
            raise Exception("{proj_home} doesnt exist".format(proj_home=proj_home))
    else:
        proj_home = os.path.abspath("./")

    if proj_home not in sys.path:
        sys.path.append(proj_home)

    conf["PROJ_HOME"] = proj_home

    conf.update(load_module(os.path.join(proj_home, "config.py")))
    conf.update(load_module(os.path.join(proj_home, "local_config.py")))
    conf_update_from_env(app_name or conf.get("SERVICE", ""), conf)

    return conf


def load_module(filename):
    """
    Loads module, first from config.py then from local_config.py
    :return dictionary
    """

    filename = os.path.join(filename)
    d = types.ModuleType("config")
    d.__file__ = filename
    try:
        with open(filename) as config_file:
            exec(compile(config_file.read(), filename, "exec"), d.__dict__)
    except IOError:
        pass
    res = {}
    from_object(d, res)
    return res


def conf_update_from_env(app_name, conf):
    app_name = app_name.replace(".", "_").upper()
    for key in list(conf.keys()):
        specific_app_key = "_".join((app_name, key))
        if specific_app_key in os.environ:
            # Highest priority: variables with app_name as prefix
            _replace_value(conf, key, os.environ[specific_app_key])
        elif key in os.environ:
            _replace_value(conf, key, os.environ[key])


def from_object(from_obj, to_obj):
    """Updates the values from the given object.  An object can be of one
    of the following two types:
    Objects are usually either modules or classes.
    Just the uppercase variables in that object are stored in the config.
    :param obj: an import name or object
    """
    for key in dir(from_obj):
        if key.isupper():
            to_obj[key] = getattr(from_obj, key)


def _replace_value(conf, key, new_value):
    try:
        w = json.loads(new_value)
        conf[key] = w
    except Exception:
        try:
            # Interpret numbers, booleans, etc...
            conf[key] = ast.literal_eval(new_value)
        except Exception:
            # String
            conf[key] = new_value


def u2asc(input):
    """
    Converts/transliterates unicode characters to ASCII, using the unidecode package.
    Functionality is similar to the legacy code in adspy.Unicode, but may treat some characters differently
    (e.g. umlauts). Standard unidecode package only handles Latin-based characters.

    :param input: string to be transliterated. Can be either unicode or encoded in utf-8
    :return output: transliterated string, in either unicode or encoded (to match input)
    """

    # TODO If used on anything but author names, add special handling for math symbols and other special chars
    test_type = str
    if not isinstance(input, test_type):
        try:
            input = input.decode("utf-8")
        except UnicodeDecodeError:
            raise UnicodeHandlerError("Input must be either unicode or encoded in utf8.")

    try:
        output = unidecode.unidecode(input)
    except UnicodeDecodeError:
        raise UnicodeHandlerError("Transliteration failed, check input.")

    if not isinstance(input, test_type):
        output = output.encode("utf-8")

    return output


#Concurrent logging setup

local_zone = tz.tzlocal()
utc_zone = tz.tzutc()

TIMESTAMP_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"

LOG_MAX_BYTES = 10485760  # 10MB
LOG_BACKUP_COUNT = 10

DEFAULT_LOG_FMT = ("%(asctime)s %(name)s %(processName)s %(filename)s %(funcName)s "
                   "%(levelname)s %(lineno)s %(module)s %(threadName)s %(message)s")


def _get_proj_home(extra_frames=0):
    """Get the location of the caller module; then go up max_levels until
    finding requirements.txt"""

    frame = inspect.stack(0)[2 + extra_frames]
    module = inspect.getsourcefile(frame[0])
    if not module:
        raise Exception("Sorry, wasnt able to guess your location. Let devs know about this issue.")
    d = os.path.dirname(module)
    x = d
    max_level = 3
    while max_level:
        f = os.path.abspath(os.path.join(x, 'requirements.txt'))
        if os.path.exists(f):
            return x
        x = os.path.abspath(os.path.join(x, '..'))
        max_level -= 1
    sys.stderr.write("Sorry, cant find the proj home; returning the location of the caller: %s\n" % d)
    return d


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
#
# Design:
#
#   logger --> _JsonQueueHandler --(SimpleQueue)--> QueueListener thread
#                                                     |--> ConcurrentRotatingFileHandler
#                                                     '--> StreamHandler(stdout) [optional]
#
# * Callers (serial code, or up to hundreds of worker threads) only do the JSON
#   formatting and a lock-free SimpleQueue.put(); they never block on file I/O
#   or on the inter-process file lock.
# * Records are formatted in the calling thread, so processName/threadName,
#   message args and exception tracebacks are captured at call time.
# * One listener thread per logger per process owns the file handler, and
#   ConcurrentRotatingFileHandler serialises writes + rotation *across*
#   processes via its lock file.
# * After os.fork() the listener thread does not exist in the child, so every
#   configured logger gets a fresh queue, fresh handlers and a fresh listener in
#   the child (os.register_at_fork).  spawn/forkserver children re-import the
#   module and simply call setup_logging() again, as before.

class _JsonQueueHandler(logging.handlers.QueueHandler):
    """QueueHandler whose prepare() renders the final JSON line in the caller's
    thread. The listener-side handlers use a pass-through '%(message)s'
    formatter, so they write exactly that line."""
    pass


class _ForkSafeQueueListener(logging.handlers.QueueListener):
    """QueueListener that holds `write_lock` while dispatching a record, so a
    fork (which takes the same lock in its `before` hook) can never happen
    while this thread is in the middle of a write. Otherwise the child would
    inherit file objects whose internal locks are held by a thread that does
    not exist in the child, and deadlock on the first touch."""

    def __init__(self, q, *handlers, write_lock, **kwargs):
        super().__init__(q, *handlers, **kwargs)
        self._write_lock = write_lock

    def handle(self, record):
        with self._write_lock:
            super().handle(record)


class _LoggingPipeline(object):
    """Book-keeping for one logger configured by setup_logging()."""

    def __init__(self, logger_name, filename, attach_stdout, formatter):
        self.logger_name = logger_name
        self.filename = filename
        self.attach_stdout = attach_stdout
        self.queue_handler = _JsonQueueHandler(queue.SimpleQueue())
        self.queue_handler.setFormatter(formatter)
        self.write_lock = threading.Lock()
        self.handlers = []
        self.listener = None

    def _build_handlers(self):
        passthrough = logging.Formatter('%(message)s')
        rfh = ConcurrentRotatingFileHandler(filename=self.filename,
                                            mode='a',
                                            maxBytes=LOG_MAX_BYTES,
                                            backupCount=LOG_BACKUP_COUNT,
                                            encoding='utf-8')
        rfh.setFormatter(passthrough)
        handlers = [rfh]
        if self.attach_stdout:
            stdout = logging.StreamHandler(sys.stdout)
            stdout.setFormatter(passthrough)
            handlers.append(stdout)
        return handlers

    def start(self):
        self.handlers = self._build_handlers()
        self.listener = _ForkSafeQueueListener(
            self.queue_handler.queue, *self.handlers,
            write_lock=self.write_lock, respect_handler_level=True)
        self.listener.start()

    def stop(self):
        """Drain the queue, then close the handlers."""
        if self.listener is not None:
            try:
                self.listener.stop()
            except Exception:
                pass
            self.listener = None
        for h in self.handlers:
            try:
                h.close()
            except Exception:
                pass
        self.handlers = []

    def reinit_after_fork(self):
        """Runs in a freshly forked child. The listener thread does not exist
        here and the queue may hold the parent's pending records (the parent
        writes those itself), so start over with a new queue, lock, handlers
        and listener. Because the fork was taken while holding write_lock, the
        inherited handlers are idle and can be closed safely (this only closes
        the child's copies of the fds; the parent is unaffected)."""
        self.write_lock = threading.Lock()
        for h in self.handlers:
            if isinstance(h, ConcurrentRotatingFileHandler):
                try:
                    h.close()
                except Exception:
                    pass
        self.queue_handler.queue = queue.SimpleQueue()
        self.listener = None
        self.start()


_pipelines = {}                 # logger name -> _LoggingPipeline
_pipelines_lock = threading.RLock()
_held_for_fork = []


def _before_fork():
    # Wait for in-flight writes to finish and block new ones until the fork is
    # done. Order: registry lock first, then pipelines by name (consistent).
    _pipelines_lock.acquire()
    for name in sorted(_pipelines):
        lock = _pipelines[name].write_lock
        lock.acquire()
        _held_for_fork.append(lock)


def _after_fork_in_parent():
    while _held_for_fork:
        _held_for_fork.pop().release()
    _pipelines_lock.release()


def _after_fork_in_child():
    global _pipelines_lock
    del _held_for_fork[:]   # the child gets brand-new locks instead
    _pipelines_lock = threading.RLock()
    for pipeline in list(_pipelines.values()):
        try:
            pipeline.reinit_after_fork()
        except Exception as exc:  # never let logging kill a worker
            sys.stderr.write('setup_logging: could not re-initialise %s after fork: %r\n'
                             % (pipeline.logger_name, exc))


@atexit.register
def _stop_all_pipelines():
    # Registered after `logging` itself, so it runs before logging.shutdown():
    # queues are drained into the files before the handlers are flushed/closed.
    with _pipelines_lock:
        for pipeline in list(_pipelines.values()):
            pipeline.stop()
        _pipelines.clear()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(before=_before_fork,
                        after_in_parent=_after_fork_in_parent,
                        after_in_child=_after_fork_in_child)


# multiprocessing (and ProcessPoolExecutor, billiard-free worker pools, ...)
# ends fork-started children with os._exit(), which skips atexit, so the
# queue would not be drained and the child's last records would be lost.
# Those children do run multiprocessing's own exit finalizers, so register
# the drain there. It has to go through register_after_fork because the
# child clears the finalizer registry right after os.fork() returns.
class _MPForkHook(object):
    pass


_mp_fork_hook = _MPForkHook()  # must stay referenced (weakref registry)


def _register_mp_child_drain(_obj):
    import multiprocessing.util
    # Negative priority: runs in the final pass, after user finalizers that
    # might still log.
    multiprocessing.util.Finalize(None, _stop_all_pipelines, exitpriority=-100)


try:
    import multiprocessing.util as _mp_util
    _mp_util.register_after_fork(_mp_fork_hook, _register_mp_child_drain)
except Exception:  # pragma: no cover - multiprocessing unavailable
    pass


def setup_logging(name_, level=None, proj_home=None, attach_stdout=False):
    """
    Sets up generic logging to file with rotating files on disk

    Safe to use from serial code, from many threads in one process and from
    many processes writing to the same log file at the same time.
    Calling it again for the same name replaces the previous configuration.

    :param: name_: the name of the logfile (not the destination!)
    :param: level: the level of the logging DEBUG, INFO, WARN (name or int)
    :param: proj_home: optional, starting dir in which we'll
            check for (and create) 'logs' folder and set the
            logger there
    :return: logging instance
    """

    # NOTE: load_config()/_get_proj_home() look at the call stack, so they must
    # stay called directly from here (not from a helper function).
    if level is None:
        config = load_config(extra_frames=1, proj_home=proj_home, app_name=name_)
        level = config.get('LOGGING_LEVEL', 'INFO')

    if isinstance(level, str):
        level = getattr(logging, level.upper())

    if proj_home:
        proj_home = os.path.abspath(proj_home)
        fn_path = os.path.join(proj_home, 'logs')
    else:
        fn_path = os.path.join(_get_proj_home(), 'logs')

    os.makedirs(fn_path, exist_ok=True)

    fn = os.path.join(fn_path, '{0}.log'.format(name_.split('.log')[0]))

    formatter = get_json_formatter()
    formatter.multiline_marker = ''
    formatter.multiline_fmt = '     %(message)s'

    logging_instance = logging.getLogger(name_)

    with _pipelines_lock:
        old = _pipelines.pop(name_, None)
        if old is not None:
            old.stop()

        pipeline = _LoggingPipeline(name_, fn, attach_stdout, formatter)
        pipeline.start()
        _pipelines[name_] = pipeline

        logging_instance.handlers = [pipeline.queue_handler]
        logging_instance.setLevel(level)
        # Do not propagate to the parent logger to avoid double logging with different formatters
        logging_instance.propagate = False

    return logging_instance



def overrides(interface_class):
    """
    To be used as a decorator, it allows the explicit declaration you are
    overriding the method of class from the one it has inherited. It checks that
     the name you have used matches that in the parent class and returns an
     assertion error if not
    """
    def overrider(method):
        """
        Makes a check that the overrided method now exists in the given class
        :param method: method to override
        :return: the class with the overriden method
        """
        assert(method.__name__ in dir(interface_class))
        return method

    return overrider


class MultilineMessagesFormatter(Formatter):
    converter = time.gmtime

    def format(self, record):
        """
        This is mostly the same as logging.Formatter.format except for adding spaces in front
        of the multiline messages.
        """
        s = Formatter.format(self, record)

        if '\n' in s:
            return '\n     '.join(s.split('\n'))
        else:
            return s

    def formatTime(self, record, datefmt=None):
        """logging uses time.strftime which doesn't understand
        how to add microsecs. datetime understands that. so we
        have to work around the old time.strftime here."""
        if datefmt:
            datefmt = datefmt.replace('%f', '%03d' % record.msecs)
        return Formatter.formatTime(self, record, datefmt)


def _ansi(code):
    def colorize(s):
        return '\033[%sm%s\033[0m' % (code, s)
    return colorize


class JsonFormatter(_BaseJsonFormatter):
    converter = time.gmtime
    #: Loglevel -> Color mapping (plain ANSI; replaces celery.utils.log.colored).
    colors = {
        'DEBUG': _ansi('34'),     # blue
        'WARNING': _ansi('33'),   # yellow
        'ERROR': _ansi('31'),     # red
        'CRITICAL': _ansi('35'),  # magenta
    }

    def __init__(self,
                 fmt=DEFAULT_LOG_FMT,
                 datefmt=TIMESTAMP_FMT,
                 use_color=False,
                 extra=None, *args, **kwargs):
        self._extra = extra or {}
        self.use_color = use_color
        super().__init__(fmt, datefmt, *args, **kwargs)

    def process_log_record(self, log_record):
        # Enforce the presence of a timestamp
        if "asctime" in log_record:
            log_record["timestamp"] = log_record["asctime"]
        else:
            log_record["timestamp"] = datetime.now(timezone.utc).strftime(TIMESTAMP_FMT)
            log_record["asctime"] = log_record["timestamp"]

        for key, value in self._extra.items():
            log_record[key] = value
        return super().process_log_record(log_record)

    def formatTime(self, record, datefmt=None):
        """logging uses time.strftime which doesn't understand
        how to add microsecs. datetime understands that. so we
        have to work around the old time.strftime here."""
        if datefmt:
            datefmt = datefmt.replace('%f', '%03d' % record.msecs)
        return Formatter.formatTime(self, record, datefmt)

    def format(self, record):
        msg = super().format(record)
        color = self.colors.get(record.levelname)
        if color and self.use_color:
            return color(msg)
        return msg


def get_json_formatter(use_color=False, logfmt=DEFAULT_LOG_FMT, datefmt=TIMESTAMP_FMT):
    return JsonFormatter(logfmt, datefmt, extra={"hostname": socket.gethostname()}, use_color=use_color)
