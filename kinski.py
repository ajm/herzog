#!/usr/bin/env python
import os
import sys
import time
import getopt
import string
import random
import threading
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

from lib.daemonbase import *
from lib.sysinfo    import Resource
from lib.fragment   import Fragment, FragmentError
from etc            import herzogdefaults

import plugins


options = {}


class WorkerThread(threading.Thread) :
    def __init__(self, fragment, done_hook) :
        self.fragment = fragment
        self.hook = done_hook

        threading.Thread.__init__(self)

    def kill(self) :
        self.fragment.kill_plugin()

    def run(self) :
        try :
            results = self.fragment.run_plugin()
            self.hook(self.fragment, True, results)

        except plugins.PluginError, pe :
            self.hook(self.fragment, False, str(pe))

class Kinski(DaemonBase) :
    
    version = 0.1

    def __init__(self, portnumber, baseworkingdir, logdir, masterhostname, masterportnumber, plugindir='plugins', cores=None, verbose=False) :
        DaemonBase.__init__(self, baseworkingdir, logdir, herzogdefaults.KINSKI_LOG_FILENAME, verbose)

        self.resource = Resource()
        self.workers = {}
        self.masterurl = "http://%s:%d" % (masterhostname, masterportnumber)
        random.seed()

        self.resource.portnumber = portnumber # <hack>
        if cores != None :
            self.resource.cpucores = cores

        plugins.init_plugins(plugindir)
        
        self.server = SimpleXMLRPCServer((self.resource.hostname, portnumber))
        
        self.server.register_function(self.list_resources,  'list_resources')
        self.server.register_function(self.fragment_prep,   'fragment_prep' )
        self.server.register_function(self.fragment_start,  'fragment_start')
        self.server.register_function(self.fragment_stop,   'fragment_stop' )
        self.server.register_function(self.fragment_list,   'fragment_list' )
        self.server.register_introspection_functions()

        self.log.debug("initialised @ http://%s:%d" % (self.resource.hostname, portnumber))

    def __fragmentisrunning(self,fragmentkey) :
        return fragmentkey in self.workers

    def __startfragment(self, fragment) :
        wt = WorkerThread(fragment, self.fragment_done)
        wt.start()

        self.workers[ fragment.key() ] = wt

    def __stopfragment(self, fragmentkey) :
        f = self.workers[fragmentkey]
        f.kill()

        del self.workers[fragmentkey]

    def __listfragments(self) :
        return map(lambda x : x.gettuple(), self.workers.keys())

    @log_functioncall
    def list_resources(self) :
        self.resource.update()
        return self.resource

    @log_functioncall
    def fragment_prep(self, project) :
        chars = string.letters + string.digits + '-'

        if False in map(lambda x : x in chars, project) :
            return (False, "%s is not a suitable project name, please limit to the following characters: %s" % (project, chars))

        projectdirectory = self.workingdirectory + os.sep + project

        try :
            path = Fragment.mk_tmp_directory(projectdirectory)
            
            return (True, path)

        except FragmentInitialisationError, fie :
            return (False, str(fie))

    @log_functioncall
    def fragment_start(self, path, program, project) :
        try :
            p = plugins.get_plugin(program)

            p.inspect_input_files(path)
            p.inspect_system(self.resource)

            f = Fragment(path, program, p, project)

            self.__startfragment(f)

        except plugins.PluginError, pe :
            return (False, str(pe))

        except FragmentError, fe :
            return (False, str(fe))
        
        return (True, f.key())

    @log_functioncall
    def fragment_stop(self, fragmentkey) :
        if self.__fragmentisrunning(fragmentkey) :
            return self.__stopfragment(fragmentkey)
        else :
            return (False, "not running")
            
    @log_functioncall
    def fragment_list(self) :
        return self.__listfragments()

    def poll_for_master(self) :
        p = xmlrpclib.ServerProxy(self.masterurl)
        while True :
            try :
                p.register_resources(self.resource)
                break
            except :
                pass

            time.sleep(60)

    def __signalmaster(self) :
        while True :
            try :
                p = xmlrpclib.ServerProxy(self.masterurl)
                p.register_resources(self.resource)
                self.log.debug("registered with master node")
                return

            except :
                self.log.critical("could not contact master node at %s" % self.masterurl)
                #sys.exit(-1)
                time.sleep(10)
                continue

    def go(self) :
        self.__signalmaster() # TODO
        self.server.serve_forever()

    def fragment_done(self, fragment, success, resultfile) :
        self.log.debug("%s %s" % (str(fragment), "success" if success else "faliure"))
        self.log.debug("resultfile = %s" % resultfile)
        try :
            p = xmlrpclib.ServerProxy(self.masterurl)
            p.fragment_complete(self.resource, fragment.project, resultfile)

        except :
            self.log.critical("could not contact master node at %s (%s)" % (self.masterurl, resultfile))
            # TODO call poll_for_master in separate thread...


def usage() :
    print >> sys.stderr, \
"""Usage: %s [-vh] [-wDIR] [-lDIR] [-pNUM] [-mHOST] [-xNUM]
    -v  verbose
    -h  print this message
    -w  working directory   (default: %s)
    -l  log directory       (default: %s)
    -p  port number         (default: %d)
    -m  master host name    (default: %s)
    -x  master port number  (default: %d)
    -n  cpu cores available (default: all)
""" %  (    sys.argv[0],                        \
            herzogdefaults.DEFAULT_WORKING_DIR, \
            herzogdefaults.DEFAULT_LOG_DIR,     \
            herzogdefaults.DEFAULT_KINSKI_PORT, \
            herzogdefaults.DEFAULT_HERZOG_HOST, \
            herzogdefaults.DEFAULT_HERZOG_PORT)

def error_msg(s) :
    print >> sys.stderr, "%s: %s" % (sys.argv[0], s)
    sys.exit(-1)

def handleargs() :
    global options
    
    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:w:l:m:x:n:")

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
        elif o == "-m" :
            options['master'] = a
        elif o == "-x" :
            try :
                portnum = int(a)
            except ValueError, e :
                error_msg("invalid port number: " + str(e))

            options['masterportnumber'] = portnum
        elif o == "-n" :
            try :
                cores = int(a)
            except ValueError, e :
                error_msg("non-integer number of cores specified: " + str(e))

            options['cores'] = cores

    if options['verbose'] :
        for k,v in options.items() :
            print "%25s\t%s" % (k,str(v))
        print ""

def main() :
    global options

    options['verbose']          = herzogdefaults.DEFAULT_VERBOSITY
    options['portnumber']       = herzogdefaults.DEFAULT_KINSKI_PORT
    options['workingdirectory'] = herzogdefaults.DEFAULT_WORKING_DIR
    options['logdirectory']     = herzogdefaults.DEFAULT_LOG_DIR
    options['master']           = herzogdefaults.DEFAULT_HERZOG_HOST
    options['masterportnumber'] = herzogdefaults.DEFAULT_HERZOG_PORT
    options['cores']            = None

    handleargs()

    try :
        k = Kinski( options['portnumber'], \
                    options['workingdirectory'], \
                    options['logdirectory'], \
                    options['master'], \
                    options['masterportnumber'], \
                    cores=options['cores'], \
                    verbose=options['verbose'])
        k.go()

    except DaemonInitialisationError, ie :
        error_msg(str(ie))
    except KeyboardInterrupt :
        sys.exit()

if __name__ == '__main__' :
    if os.name != 'posix' :
        print >> sys.stderr, "%s is only supported on posix systems at the moment" % sys.argv[0]

    main()

