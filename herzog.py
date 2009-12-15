#!/usr/bin/env python
import os
import sys
import time
import signal
import getopt
import socket
import xmlrpclib
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

    def __init__(self, portnumber, baseworkingdir, logdir, username='herzog', verbose=False) :
        DaemonBase.__init__(self, baseworkingdir, logdir, herzogdefaults.HERZOG_LOG_FILENAME, verbose)

        self.resources = ResourcePool()
        self.projects = ProjectPool()
        self.username = username
        
        url = "http://%s:%d" % (socket.gethostname(), portnumber)
        hn = socket.gethostname()
        hn = 'euclid.kleta-lab'
        
        self.server = SimpleXMLRPCServer((hn, portnumber))
        
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

        self.server.register_function(self.list_current,    'list_current')
        
        self.log.debug("initialised @ %s" % url)

    @log_functioncall
    def project_add(self, args) :
        name,path,program = args

        if self.projects.exists(name) :
            msg = "a project called %s already exists" % name
            self.log.info("project_add: " + msg)
            return (False, msg)

        try :
            p = Project(name, path, program)

        except ProjectError, pe:
            self.log.info(str(pe))
            return (False, str(pe))

        self.projects.put_project(p)
        p.process_background()
        
        self.log.info("project_add: %s (%d fragments) running %s from %s" % (name, p.total_fragments, program, path))
        
        return (True,'processing %d fragments' % p.total_fragments)
    
    @log_functioncall
    def project_remove(self, args) : 
        name = args[0]

        # TODO: this was originally intended to delete the project from the scheduler
        # then send out rpcs to stop any currently running jobs
        try :
            p = self.projects.get_project(name)

            if self.scheduler.running(name) :
                self.scheduler.cancel_current_project()

            self.projects.remove(name)

        except ProjectError, pe :
            self.log.info("project_remove: " + str(pe))
            return (False, str(pe))

        self.log.debug("deleted %s project" % name)

        return (True,'removed %s' % name)

    @log_functioncall
    def project_pause(self, args) :
        name = args[0]

        try :
            self.projects.pause(name)

        except ProjectError, pe :
            self.log.info("project_pause: " + str(pe))
            return (False, str(pe))

        self.log.debug("paused %s" % name)

        return (True,'paused %s' % name)

    @log_functioncall
    def project_resume(self, args) :
        name = args[0]

        try :
            self.projects.resume(name)

        except ProjectError, pe :
            self.log.info("project_resume: " + str(pe))
            return (False, str(pe))

        self.log.debug("resumed %s" % name)

        return (True,'resumed %s' % name)

    @log_functioncall
    def project_progress(self, args) :

        if len(args) == 0 :
            names = self.projects.get_project_names()
        else :
            names = args

        tmp = {}

        for name in names :
            try :
                p = self.projects.get_project(name)
                state,progress = p.progress()
                tmp[name] = (state, "%.2f%%" % progress)

                self.log.debug("\t%s %s %s" % (name, tmp[name][0], tmp[name][1]))
                
            except ProjectError, pe :
                continue

        return (True, tmp)

    @log_functioncall
    def estimate_completion(self, args) :
        # TODO this would depend on the scheduler implementation
        # this is only for the FIFOScheduler, should move to scheduler class
        name = args[0]

        try :
            p = self.projects.get_project(name)
            state,prog = p.progress()

            if state != 'running' :
                return (False, 'project \'%s\' has not properly started yet' % name)

            if prog == 0 :
                return (False, 'project \'%s\' has not properly started yet' % name)

            end_time = p.start_time + ((time.time() - p.start_time) * (100 / prog))

        except ProjectError, pe :
            return (False, str(pe))

        end = time.ctime(end_time)

        self.log.debug("\t%s finish @ %s" % (name, end))
        return (True, time.ctime(end_time))

    @log_functioncall
    def scheduler_policy(self, args) :
        # TODO reinstantiate self.scheduler + pass it references to projects, resources
        return (False, 'herzog only supports a FIFOScheduler at the moment')

    @log_functioncall
    def fragment_complete(self, resource, project, remotepath) :
        self.resources.add_core_resource(resource['hostname'])
        p = self.projects.get_project(project)

        # <hack>
        #   get the results file, 
        #   I need get the SCORE file from path,
        #   path needs to resolve from remotepath -> localpath/SCORE-XX_YYY.ALL
        localpath,hostname = p.mapping_get( os.path.dirname(remotepath) )
        self.get_resultfile(resource['hostname'], remotepath, localpath)
        # </hack>
        
        p.fragment_complete()

        return (True, '')
    
    @log_functioncall
    def register_resources(self, resource) :
        self.resources.add_host_resource(resource)

        self.log.debug("\tresource added")
        for k,v in resource.items() :
            self.log.debug("%20s  %s" % (str(k), str(v)))

        return (True, '')

    # XXX this is a temporary thing, 
    # just to get a peek at some internal state
    @log_functioncall
    def list_current(self) :
        return self.scheduler.current_project.map
    
    def transfer_datafiles(self, hostname, remotepath, localpath) :
        command = "scp %s/*DAT %s@%s:%s" % (localpath, self.username, hostname, remotepath)
        if 0 != os.system(command) :
            raise DaemonError("could not tx files with \"%s\"" % command)
    
    def get_resultfile(self, hostname, remotepath, localpath) :
        command = "scp %s@%s:%s %s" % (self.username, hostname, remotepath, localpath)
        if 0 != os.system(command) :
            raise DaemonError("could retrieve results file with \"%s\"" % command)

    def get_proxy(self,r) :
        return xmlrpclib.ServerProxy("http://%s:%d" % (r['hostname'], r['portnumber'])) 
        # TODO: put port number in resource object from kinski

    def launch_job(self, resource, job) :
        proxy       = self.get_proxy(resource)
        project     = job.project
        path        = job.path
        program     = job.program
        
        successful,tmpdir = proxy.fragment_prep( project )
        if not successful :
            raise DaemonError(tmpdir)

        # <hack>
        # nasty hack of a mapping between remote directory and where we want the local 
        # results file to sit and be called...
        p = self.projects.get_project(project)
        p.mapping_put( tmpdir, (job.resultsfile,resource['hostname']) )
        self.log.debug("*** mapping %s -> %s" % (tmpdir, job.resultsfile))
        # </hack>

        self.transfer_datafiles(resource['hostname'], tmpdir, path)

        successful,msg = proxy.fragment_start( tmpdir, program, project )
        if not successful :
            raise DaemonError(msg)
        
    def go(self) :
        self.scheduler_thread = threading.Thread(target=self.main_loop)
        self.xmlrpc_thread    = threading.Thread(target=self.rpc_loop)

        self.scheduler_thread.start()
        self.xmlrpc_thread.start()

    def rpc_loop(self) :
        try :
            self.server.serve_forever()
            
        except :
            pass

    def main_loop(self) :
        self.scheduler = FIFOScheduler(self.projects, self.resources)
        keep_job = False

        while True :
            #r,j = self.scheduler.get_resource_job()
            r = self.scheduler.get_resource()
            if not keep_job :
                j = self.scheduler.get_job()
                keep_job = False

            try :
                self.log.debug("scheduler: %s @ %s" % (str(j), r['hostname']))
                self.launch_job(r,j)
                continue
                
            except DaemonError, de :
                self.log.error(str(de))
            except xmlrpclib.Fault, fau :
                self.log.error(str(fau))
            except socket.gaierror, gai :
                self.log.error(str(gai))

            keep_job = True


def usage() :
    print >> sys.stderr, \
"""Usage: %s [-vh] [-wDIR] [-lDIR] [-pNUM]
    -v  verbose
    -h  print this message
    -w  working directory   (default: %s)
    -l  log directory       (default: %s)
    -p  port number         (default: %d)
    -u  user name           (default: %s)
""" %  (    sys.argv[0],                        \
            herzogdefaults.DEFAULT_WORKING_DIR, \
            herzogdefaults.DEFAULT_LOG_DIR,     \
            herzogdefaults.DEFAULT_HERZOG_PORT, \
            herzogdefaults.DEFAULT_USERNAME)

def error_msg(s) :
    print >> sys.stderr, "%s: %s" % (sys.argv[0], s)
    sys.exit(-1)

def handleargs() :
    global options

    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:w:l:u:")

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
        elif o == "-u" :
            options['username'] = a

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
    options['username']         = herzogdefaults.DEFAULT_USERNAME

    handleargs()

    try :
        h = Herzog( options['portnumber'], \
                    options['workingdirectory'], \
                    options['logdirectory'], \
                    username=options['username'], \
                    verbose=options['verbose'])
        h.go()

    except DaemonInitialisationError, ie :
        error_msg(str(ie))

    try :
        signal.pause()

    except KeyboardInterrupt :
        sys.exit()


if __name__ == '__main__' :
    if os.name != 'posix' :
        print >> sys.stderr, "%s is only supported on posix systems at the moment" % sys.argv[0]
    main()

