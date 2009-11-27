import logging
import logging.handlers

def get_logger(loggername, logdirname, logfilename, verbose=False) :

    logname = "%s/%s" % (logdirname, logfilename)

    log = logging.getLogger(loggername)
    log.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if verbose else logging.ERROR)
    console.setFormatter(logging.Formatter("%(name)s:%(message)s"))

    logfile = logging.handlers.RotatingFileHandler(logname, maxBytes=2**20, backupCount=5)
    logfile.setLevel(logging.DEBUG)
    logfile.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    log.addHandler(console)
    log.addHandler(logfile)

    return log

