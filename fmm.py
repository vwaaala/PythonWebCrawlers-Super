import logging

#logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mleasing_crawler")
# DEBUG, INFO, WARNING, ERROR, CRITICAL
log.setLevel(logging.DEBUG)
# log.setLevel(logging.WARNING)
#log.setLevel(logging.ERROR)

# create formatter and add it to the handlers
formatter = logging.Formatter(
            '%(asctime)s %(levelname)s %(name)s  %(message)s')

# # create file handler that logs debug and higher level messages
# fh = logging.FileHandler('spam.log')
# fh.setLevel(logging.DEBUG)
# fh.setFormatter(formatter)
# # add the handlers to logger
# logger.addHandler(fh)

# create console handler with a higher log level
ch = logging.StreamHandler()
# ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
# add the handlers to logger
log.addHandler(ch)



log.debug('sss')
