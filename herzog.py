#!/usr/bin/env python
import os
import sys
import getopt
import socket
import threading
from SimpleXMLRPCServer import SimpleXMLRPCServer

from lib.projectentities  import Project, ProjectError, ProjectPool, Job
from lib.resourceentities import ResourcePool
from lib.daemonbase import *
from lib.sysinfo    import Resource
from lib.scheduler  import FIFOScheduler
from etc            import herzogdefaults


options = {}

# TODO :
#
# add periodic checks that :
#   resources are still active, 
#   whether things need to go back on a queue,
#   that rpcs were returned correctly
#
# this can be the same code that is run from the beginning given a restart!

class Herzog(DaemonBase) :

    version = 0.1

    def __init__(self, portnumber, baseworkingdir, logdir, verbose=False) :
        DaemonBase.__init__(self, baseworkingdir, logdir, herzogdefaults.HERZOG_LOG_FILENAME, verbose)

        self.resources = ResourcePool()
        self.projects = ProjectPool()
        self.scheduler = FIFOScheduler(self.projects, self.resources)
        
        url = "http://%s:%d" % (socket.gethostname(), portnumber)
        
        self.server = SimpleXMLRPCServer((socket.gethostname(), portnumber))
        
        self.server.register_function(self.register_resources,  'register_resources')
        self.server.register_function(self.fragment_complete,   'fragment_complete')
        self.server.register_function(self.project_add,         'project_add')
        self.server.register_function(self.project_remove,      'project_remove')
        self.server.register_function(self.project_pause,       'project_pause')
        self.server.register_function(self.project_resume,      'project_resume')
        self.server.register_function(self.project_progress,    'project_progress')
        self.server.register_function(self.estimate_completion, 'estimate_completion')
        self.server.register_function(self.scheduler_policy,    'scheduler_policy')
        self.server.register_introspection_functions()
        
        self.log.debug("initialised @ %s" % url)

    @log_functioncall
    def project_add(self, args) :
        name,path = args
        try :
            p = Project(name, path)

        except ProjectError, pe:
            return (False, str(pe))

        # TODO 
        # create project
        # perform quick verification checks
        # spawn thread to process entire genome + add to queue
        # return from rpc

        return (True,'')

    @log_functioncall
    def project_remove(self, args) : 
        name = args[0]

        # TODO
        # find project - check it exists
        # find fragments that are running
        #   send rpcs to stop fragments
        # delete project from memory - keep files though... or rename?

        return (True,'')

    @log_functioncall
    def project_pause(self, args) :
        name = args[0]

        # TODO
        # check project exists
        # remove project from queue of eligable projects

        return (True,'')

    @log_functioncall
    def project_resume(self, args) :
        name = args[0]

        # TODO
        # check project exists
        # add project back to queue of eligible projects

        return (True,'')

    @log_functioncall
    def project_progress(self, args) :

        # TODO
        # for each project named 
        # find each project + return proportion complete
        #       -- as a dict, { projectname : float }

        return (True,'')

    @log_functioncall
    def estimate_completion(self, args) :
        # TODO this would depend on the scheduler implementation
        return (True, 'never')

    @log_functioncall
    def scheduler_policy(self, args) :
        # TODO reinstantiate self.scheduler + pass it references to projects, resources
        return (False, 'herzog only supports a FIFOScheduler at the moment')

    @log_functioncall
    def fragment_complete(self, resource) : 
        self.resources.add_core_resource(resource['hostname'])
        return (True, '')

    @log_functioncall
    def register_resources(self, resource) :
        self.resources.add_host_resource(resource)
        return (True, '')

    def transfer_datafiles(path, hostname, tmpdir) :
        command = "scp %s/* %s:%s" % (path, hostname, tmpdir)
        if 0 != os.system(command) :
            raise DaemonError("could not tx files with \"%s\"" % command)
    
    def get_proxy(r) :
        return xmlrpclib.ProxyServer("http://%s:%d" % (r['hostname'], herzogdefaults.DEFAULT_KINSKI_PORT)) # TODO: put port number in resource object from kinski

    def launch_job(self, resource, job) :
        proxy = get_proxy(r)
        project     = job.project
        path        = job.path
        program     = job.program
        
        successful,tmpdir = proxy.fragment_prep( project )
        if not successful :
            raise DaemonError(tmpdir)

        transfer_datafiles(path, r['hostname'], tmpdir)

        successful,msg = proxy.fragment_start( tmpdir, program )
        if not successful :
            raise DaemonError(msg)
        
    def go(self) :
        self.xmlrpc_thread = threading.Thread(target=self.server.serve_forever())
        self.scheduler_thread = threading.Thread(target=self.main_loop)

        self.xmlrpc_thread.start()
        self.scheduler_thread.start()

    def main_loop(self) :
        while True :
            r,j = self.scheduler.get_resource_job()

            try :
                self.launch_job(r,j)
                
            except DaemonError, de :
                self.log.error(str(de))
            except xmlrpclib.Fault, fau :
                self.log.error(str(fau))
            except socket.gaierror, gai :
                self.log.error(str(gai))


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

    try :
        os.wait()
    except KeyboardInterrupt :
        sys.exit()


if __name__ == '__main__' :
    if os.name != 'posix' :
        print >> sys.stderr, "kinski is only supported on posix systems at the moment"
    main()

