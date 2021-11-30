import logging
import os


def init_log(logger, log_level):
    # create formatter and add it to the handlers
    formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d %(levelname)-.1s %(name)s: %(message)s', 
            '%m-%d-%Y %H:%M:%S')

    if os.getenv("ISCRONTAB"):
        # create file handler that logs debug and higher level messages
        fh = logging.FileHandler(os.path.join("/tmp", logger.name + ".log"))
        fh.setLevel(log_level)
        fh.setFormatter(formatter)
        # add the handlers to logger
        logger.addHandler(fh)
        logger.setLevel(log_level)
    else:
        # create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(log_level)
        ch.setFormatter(formatter)
        # add the handlers to logger
        logger.addHandler(ch)
        logger.setLevel(log_level)
