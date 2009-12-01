#!/usr/bin/env python
import os
import sys
import getopt
import socket
from SimpleXMLRPCServer import SimpleXMLRPCServer

from lib.daemonbase import *
from lib.sysinfo    import Resource
from lib.scheduler  import FairScheduler
from etc            import herzogdefaults


options = {}


class Herzog(DaemonBase) :

    version = 0.1

    def __init__(self, portnumber, baseworkingdir, logdir, verbose=False) :
        DaemonBase.__init__(self, baseworkingdir, logdir, herzogdefaults.HERZOG_LOG_FILENAME, verbose)
        self.resources = {}
        self.projects = {}
        self.scheduler = FairScheduler(self.projects, self.resources)

        url = "http://%s:%d" % (socket.gethostname(), portnumber)
        
        self.server = SimpleXMLRPCServer((socket.gethostname(), portnumber))

        self.server.register_function(self.register_resources,  'register_resources')
        self.server.register_function(self.fragment_complete,   'fragment_complete')
        self.server.register_function(self.project_add,         'project_add')
        self.server.register_function(self.project_remove,      'project_remove')
        self.server.register_function(self.project_pause,       'project_pause')
        self.server.register_function(self.project_resume,      'project_resume')
        self.server.register_function(self.project_progress,    'project_progress')
        self.server.register_introspection_functions()

        self.log.debug("initialised @ %s" % url)

    @log_functioncall
    def project_add(self, name, path) : 
        pass

    @log_functioncall
    def project_remove(self, name) : 
        pass

    @log_functioncall
    def project_pause(self, name) :
        pass

    @log_functioncall
    def project_resume(self, name) :
        pass

    @log_functioncall
    def project_progress(self) : 
        pass

    @log_functioncall
    def fragment_complete(self) : 
        pass

    @log_functioncall
    def register_resources(self) : 
        pass

    def go(self) :
        self.server.serve_forever()


def usage() :
    print >> sys.stderr, \
"""Usage: %s [-vh] [-wDIR] [-lDIR] [-pNUM]
    -v  verbose
    -h  print this message
    -w  working directory   (default: %s)
    -l  log directory       (default: %s)
    -p  port number         (default: %d)
""" %  (    sys.argv[0],                        \
            herzogdefaults.DEFAULT_WORKING_DIR, \
            herzogdefaults.DEFAULT_LOG_DIR,     \
            herzogdefaults.DEFAULT_HERZOG_PORT)

def error_msg(s) :
    print >> sys.stderr, "%s: %s" % (sys.argv[0], s)
    sys.exit(-1)

def handleargs() :
    global options

    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:w:l:")

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
                error_msg("invalid port number: " + str(e))

            options['portnumber'] = portnum
        elif o == "-w" :
            if not os.path.exists(a) :
                error_msg("%s does not exist" % a)

            options['workingdirectory'] = a
        elif o == "-l" :
            if not os.path.exists(a) :
                error_msg("%s does not exist" % a)

            options['logdirectory'] = a

    if options['verbose'] :
        for k,v in options.items() :
            print "%25s\t%s" % (k,str(v))
        print ""

def main() :
    global options

    options['verbose']          = herzogdefaults.DEFAULT_VERBOSITY
    options['portnumber']       = herzogdefaults.DEFAULT_HERZOG_PORT
    options['workingdirectory'] = herzogdefaults.DEFAULT_WORKING_DIR
    options['logdirectory']     = herzogdefaults.DEFAULT_LOG_DIR

    handleargs()

    try :
        h = Herzog( options['portnumber'], \
                    options['workingdirectory'], \
                    options['logdirectory'], \
                    verbose=options['verbose'])
        h.go()

    except DaemonInitialisationError, ie :
        error_msg(str(ie))
    except KeyboardInterrupt :
        sys.exit()


if __name__ == '__main__' :
    if os.name != 'posix' :
        print >> sys.stderr, "kinski is only supported on posix systems at the moment"
    main()

