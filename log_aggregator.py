"""
Created on Thu Dec  9 16:30:43 2015

@author: Lior Redlus
"""

from logging import Handler
from threading import RLock, Timer
from collections import defaultdict


class LogAggregatorHandler(Handler):

    _default_flush_timer = 300  # Number of seconds between consecutive flushes
    _default_separator = "\t"  # Seperator character between metadata strings
    _default_metadata = ["filename", "name", "funcName", "lineno", "levelname"]  # metadata defining unique log records

    class LogAggregatorCache(object):
        """ Keeps whatever is interesting in log records for aggregation. """
        def __init__(self, record=None):
            self.message = None
            self.counter = 0
            self.timestamp = list()
            self.args = list()
            if record is not None:
                self.cache(record)

        def cache(self, record):
            if self.message is None:  # Only the first message is kept
                self.message = record.msg
            assert self.message == record.msg, "Non-matching log record"  # note: will not work with string formatting for log records; e.g. "blah {}".format(i)
            self.timestamp.append(record.created)
            self.args.append(record.args)
            self.counter += 1
        
        def __str__(self):
            """ The string of this object is used as the default output of log records aggregation. For example: record message with occurrences. """
            return self.message + "\t (occurred {} times)".format(self.counter)

    def __init__(self, flush_timer=None, separator=None, add_process_thread=False):
        """
        Log record metadata will be concatenated to a unique string, separated by self._separator.
        Process and thread IDs will be added to the metadata if set to True; otherwise log records across processes/threads will be aggregated together.
        :param separator: str
        :param add_process_thread: bool
        """
        super().__init__()
        self._flush_timer = flush_timer or self._default_flush_timer
        self._cache = self.cache_factory()  # each dictionary holds the log types found under some metadata
        self._separator = separator or self._default_separator
        self._metadata = self._default_metadata
        if add_process_thread is True:
            self._metadata += ["process", "thread"]
        self._aggregation_lock = RLock()
        self._store_aggregation_timer = self.flush_timer_factory()
        self._store_aggregation_timer.start()
        
        # Demo logger which outputs aggregations through the StreamHandler:
        self.agg_log = logging.getLogger("aggregation_logger")
        self.agg_log.addHandler(logging.StreamHandler())
        self.agg_log.setLevel(logging.DEBUG)
        self.agg_log.propagate = False
    
    def cache_factory(self):
        """ Returns an instance of a new caching object. """
        return defaultdict(self.LogAggregatorCache)
    
    def flush_timer_factory(self):
        """ Returns a threading.Timer daemon object which flushes the Handler aggregations. """
        timer = Timer(self._flush_timer, self.flush)
        timer.daemon = True
        return timer
    
    def find_unique(self, record):
        """ Extracts a unique metadata string from log records. """
        metadata = ""
        for single_metadata in self._metadata:
            value = getattr(record, single_metadata, "missing " + str(single_metadata))
            metadata += str(value) + self._separator
        return metadata[:-len(self._separator)]

    def emit(self, record):
        try:
            with self._aggregation_lock:
                metadata = self.find_unique(record)
                self._cache[metadata].cache(record)
        except Exception:
            self.handleError(record)
        
    def flush(self):
        self.store_aggregation()
    
    def store_aggregation(self):
        """ Write the aggregation data to file. """
        self._store_aggregation_timer.cancel()
        del self._store_aggregation_timer
        with self._aggregation_lock:
            temp_aggregation = self._cache
            self._cache = self.cache_factory()
            
        # ---> handle temp_aggregation and write to file <--- #
        for key, value in sorted(temp_aggregation.items()):
            self.agg_log.info("{}\t{}".format(key, value))
        
        # ---> re-create the store_aggregation Timer object <--- #
        self._store_aggregation_timer = self.flush_timer_factory()
        self._store_aggregation_timer.start()

if __name__ == "__main__":
    import random
    import logging

    logger = logging.getLogger()
    handler = LogAggregatorHandler()
    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    logger.info("entering logging loop")

    for i in range(25):
        # Randomly choose log severity:
        severity = random.choice([logging.DEBUG, logging.INFO, logging.WARN, logging.ERROR, logging.CRITICAL])
        logger.log(severity, "test message number %s", i)

    logger.info("end of test code")
