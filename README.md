# Python Log Aggregator

A framework for aggregating similar log files using a subclass of logging.Handler.
A unique metadata string is constructed from each log record, and its message, variables and timestamp are kept in a cache.
A timer (default = 300 secs) flushes the cache into self.agg_log, which in this demo displays aggregations on the screen using logging.StreamHandler.
