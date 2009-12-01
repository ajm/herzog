import os
import sys
import logging
import logging.handlers

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

    def __init__(self, workingdir, logdirname, logfilename, verbose) :
        try :
            self.__setup_logging(logdirname, logfilename, verbose)
            self.__setupworkingdirectory(workingdir)
        except DaemonInitialisationError, die :
            print >> sys.stderr, "Initialisation error: %s" % str(die)
            sys.exit(-1)
    
    def __setup_logging(self, logdirname, logfilename, verbose) :
        try :
            self.log = self.__get_logger(self.__class__.__name__, logdirname, logfilename)

        except OSError, ose :
            raise DaemonInitialisationError(str(ose))
        except IOError, ioe :
            raise DaemonInitialisationError(str(ioe))

    def __setupworkingdirectory(self, wd) :
        if not os.access(wd, os.F_OK | os.R_OK | os.W_OK) :
            raise DaemonInitialisationError("working directory %s does not exist or I do not have read/write permission" % wd)

        if wd.startswith(".") :
            wd = os.getcwd() + os.sep + wd
        
        workingdir = wd + os.sep + self.__class__.__name__.lower()

        if os.path.exists(workingdir) :
            if not os.access(workingdir, os.F_OK | os.R_OK | os.W_OK) :
                raise DaemonInitialisationError("%s exists, but I do not have permission to access it" % workingdir)
        else :
            try :
                os.mkdir(workingdir)
                os.chmod(workingdir, 0777)

            except OSError, ose :
                raise DaemonInitialisationError("could not create %s" % workingdir)

        self.workingdirectory = workingdir

    def __get_logger(self, loggername, logdirname, logfilename, verbose=False) :

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

