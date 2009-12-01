import logging
import logging.handlers
import os

class DaemonError(Exception) :
    pass

class DaemonInitialisationError(Exception) :
    pass

def log_functioncall(f):
    argnames = f.func_code.co_varnames[:f.func_code.co_argcount]

    def new_f(*args) :
        args[0].log.info("%s(%s)" % (f.func_name, ', '.join('%s=%r' % entry for entry in zip(argnames[1:],args[1:]))))
        return f(*args)

    return new_f

class DaemonBase :

    def __init__(self, logdirname, logfilename, verbose) :
        self.__setup_logging(logdirname, logfilename, verbose)
    
    def __setup_logging(self, logdirname, logfilename, verbose) :
        try :
            self.log = self.get_logger(self.__class__.__name__, logdirname, logfilename)

        except OSError, ose :
            raise DaemonInitialisationError(str(ose))
        except IOError, ioe :
            raise DaemonInitialisationError(str(ioe))

    def get_logger(self, loggername, logdirname, logfilename, verbose=False) :

        logname = "%s%s%s" % (logdirname, os.sep, logfilename)

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

