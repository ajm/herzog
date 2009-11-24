#!/usr/bin/env python
import sys, getopt, threading, signal, time
from SimpleXMLRPCServer import SimpleXMLRPCServer
import logging, logging.handlers

from sysinfo import resource

options = {}

class WorkerThread(threading.Thread) :
    def __init__(self) :
        pass

    def run() :
        pass

class SimwalkWorkerThread(WorkerThread) :
    def __init__(self) :
        pass

class GenehunterWorkerThread(WorkerThread) :
    def __init__(self) :
        pass

class Kinski :
    
    version = '0.1'

    def __init__(self, portnumber, verbose=False) :
        self.setuplogging(verbose)
        self.resource = resource()
        self.workers = {}

        self.server = SimpleXMLRPCServer((self.resource.hostname, portnumber))
        
        self.server.register_function(self.resources_list,  'resources_list')
        self.server.register_function(self.fragment_start,  'fragment_start')
        self.server.register_function(self.fragment_stop,   'fragment_stop' )
        self.server.register_function(self.fragment_list,   'fragment_list' )
        self.server.register_introspection_functions()

        self.log.debug("initialised")

    def name(self):
        return self.__class__.__name__

    def setuplogging(self, verbose) :
        self.logname = "/var/log/%s.log" % self.name()
        self.log = logging.getLogger(self.name())
        self.log.setLevel(logging.INFO)

        if verbose :
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            console.setFormatter(logging.Formatter("%(name)s:%(message)s"))

            self.log.addHandler(console)
        
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        loghandler = logging.handlers.RotatingFileHandler(self.logname, maxBytes=2**20, backupCount=5)
        loghandler.setFormatter(formatter)

        self.log.addHandler(loghandler)

    def log_functioncall(f):
        argnames = f.func_code.co_varnames[:f.func_code.co_argcount]

        def new_f(*args) :
            args[0].log.info("%s(%s)" % (f.func_name, ', '.join('%s=%r' % entry for entry in zip(argnames[1:],args[1:]))))
            return f(*args)

        return new_f

    @log_functioncall
    def resources_list(self) :
        self.resource.update()
        return self.resource

    @log_functioncall
    def fragment_start(self, path_to_fragment, program) :
        if program.lower() != "simwalk" :
            return (False, "%s v%s only accepts simwalk fragments" % (self.name(), Kinski.version))

        return (True, '')

    @log_functioncall
    def fragment_stop(self) :
        pass

    @log_functioncall
    def fragment_list(self) :
        pass

    def go(self) :
        self.server.serve_forever()

    def fragment_done(id) :
        pass


def usage() :
    print >> sys.stderr, "Usage: %s [-vh] [-pNUM]\n\t-v\tverbose\n\t-h\tprint this message\n\t-p\tport number" % sys.argv[0]

def handleargs() :
    global options
    
    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:")
    except getopt.GetoptError, err:
        print >> sys.stderr, str(err)
        usage()
        sys.exit(-1)

    for o,a in opts :
        if o == "-v" :
            options['verbose'] = True
        elif o == "-h" :
            usage()
            sys.exit()
        elif o == "-p" :
            try :
                portnum = int(a)
            except ValueError, e :
                print >> sys.stderr, "invalid port number: " + str(e)
                sys.exit(-1)
            options['portnumber'] = portnum

def main() :
    global options

    options['verbose'] = False
    options['portnumber'] = 8000

    handleargs()

    k = Kinski(options['portnumber'], verbose=options['verbose'])

    # SimpleXMLRPCServer deadlock if serve_forver and shutdown are called in the same thread...
    #t = threading.Thread(target=k.go)
    #t.start()

    try :
        k.go()
    except KeyboardInterrupt :
        sys.exit()

if __name__ == '__main__' :
    main()

