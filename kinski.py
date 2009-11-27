#!/usr/bin/env python
import sys, getopt, threading, os, string, random
import logging, logging.handlers
from SimpleXMLRPCServer import SimpleXMLRPCServer
import xmlrpclib

from sysinfo import resource
import herzogdefaults
import utils
import plugins

###
# Questions:
#   how is each thread going to log, does it need to instantiate the same logger? pass it? - probably not thread safe...
#   how am i going to use FragmentTokens? just as keys with kinski to access worker threads? should they be passed to kinski from kerzog?
#   does the plugin stuff work if the daemon is started from an init script? - i only used a relative path
#   
###

options = {}

class WorkerThread(threading.Thread) :
    def __init__(self, kinski, fragmentkey, pluginobj, path) :
        threading.Thread.__init__(self)
        self.log = utils.get_logger('workerthread', '.', herzogdefaults.KINSKI_LOG_FILENAME, verbose=True)
        self.program = pluginobj
        self.path = path
        self.fk = fragmentkey
        self.kinski = kinski

    def kill(self) :
        self.program.kill()

    def run(self) :
        self.log.debug("thread running %s @ %s" % (self.fk.program, self.fk.path))
        resultsfile = ''
        success = True
        try :
            resultsfile = self.program.run(self.path)

        except plugins.PluginError, pe :
            self.log.debug(str(pe))
            success = False

        self.kinski.fragment_done(self.fk, success, resultsfile)


class KinskiError(Exception) :
    pass

class KinskiInitialisationError(Exception) :
    pass

class FragmentInitialisationError(Exception) :
    pass


class Fragment :
    def __init__(self,path):
        self.projectdirectory   = path
        self.__makesubdir()

    def getfragmentdirectory(self) :
        return self.fragmentdir

    def __makesubdir(self) :
        # ensure the project directory exists...
        try :
            if not os.path.isdir(self.projectdirectory) :
                os.mkdir(self.projectdirectory)
        except OSError, ose :
            raise FragmentInitialisationError(str(ose))

        chars = string.letters + string.digits

        # create a directory with a randomly generated name
        # to hold the temp input + output files
        while True :
            randomdir = ''.join(map(lambda x : chars[int(random.random() * len(chars))], range(8)))
            randomdir = self.projectdirectory + os.sep + randomdir
            if not os.path.exists(randomdir) :
                try :
                    os.mkdir(randomdir)
                except OSError, ose :
                    raise FragmentInitialisationError(str(ose))
                break

        self.fragmentdir = randomdir
    
    def __str__(self):
        return self.fragmentdir


class FragmentKey :
    def __init__(self, hostname, path, program) :
        self.hostname   = hostname
        self.path       = path
        self.program    = program

    def gettuple(self) :
        return (self.hostname, self.path, self.program)

    def __str__(self) :
        tup = self.gettuple()
        return "fk" + ((":%s"*len(tup)) % tup)


class Kinski :
    
    version = '0.1'

    def __init__(self, portnumber, baseworkingdir, logdir, masterhostname, masterportnumber, plugindir='plugins', verbose=False) :
        self.__setuploggingdirectory(logdir, verbose)
        self.__setupworkingdirectory(baseworkingdir)

        self.resource = resource()
        self.workers = {}
        self.masterurl = "http://%s:%d" % (masterhostname, masterportnumber)
        random.seed()

        plugins.init_plugins(plugindir)
        
        self.server = SimpleXMLRPCServer((self.resource.hostname, portnumber))
        
        self.server.register_function(self.list_resources,  'list_resources')
        self.server.register_function(self.fragment_prep,   'fragment_prep' )
        self.server.register_function(self.fragment_start,  'fragment_start')
        self.server.register_function(self.fragment_stop,   'fragment_stop' )
        self.server.register_function(self.fragment_list,   'fragment_list' )
        self.server.register_introspection_functions()

        self.log.debug("initialised @ http://%s:%d" % (self.resource.hostname, portnumber))

    def name(self):
        return self.__class__.__name__.lower()

    def __setuploggingdirectory(self, logdir, verbose) :
        try :
            self.log = utils.get_logger('kinski', logdir, herzogdefaults.KINSKI_LOG_FILENAME, verbose=verbose)

        except OSError, ose :
            raise KinskiInitialisationError(str(ose))
        except IOError, ioe :
            raise KinskiInitialisationError(str(ioe))

    def __setupworkingdirectory(self, wd) :
        if not os.access(wd, os.F_OK | os.R_OK | os.W_OK) :
            raise KinskiInitialisationError("working directory %s does not exist or I do not have read/write permission" % wd)

        self.workingdir = wd

    def __fragmentisrunning(self,fragmentkey) :
        return fragmentkey in self.workers

    def __startfragment(self, fragmentkey, plugin, path) :
        wt = WorkerThread(self, fragmentkey, plugin, path)
        wt.start()

        self.workers[fragmentkey] = wt

    def __stopfragment(self,fragmentkey) :
        f = self.workers[fragmentkey]
        f.kill()
        del self.workers[fragmentkey]

    def __listfragments(self) :
        return map(lambda x : x.gettuple(), self.workers.keys())

    def log_functioncall(f):
        argnames = f.func_code.co_varnames[:f.func_code.co_argcount]

        def new_f(*args) :
            args[0].log.info("%s(%s)" % (f.func_name, ', '.join('%s=%r' % entry for entry in zip(argnames[1:],args[1:]))))
            return f(*args)

        return new_f

    @log_functioncall
    def list_resources(self) :
        self.resource.update()
        return self.resource

    @log_functioncall
    def fragment_prep(self, project) :
        if False in map(lambda x : x in chars, project)
            return (False, "%s is not a suitable project name, please limit to %s" % chars)

        projectdirectory = self.workingdir + os.sep + project

        try :
            f = Fragment(projectdirectory)
            return (True, f.getfragmentdirectory())

        except FragmentInitialisationError, fie :
            return (False, str(fie))

    @log_functioncall
    def fragment_start(self, path, program) :
        try :
            p = plugins.get_plugin(program)

            p.inspect_input_files(path)
            p.inspect_system(self.resource)

        except plugins.PluginError, pe :
            return (False, str(pe))

        fk = FragmentKey(self.resource.hostname, path, program)

        self.__startfragment(fk, p, path)
        
        return (True, fk)

    @log_functioncall
    def fragment_stop(self, fragmentkey) :
        if self.__fragmentisrunning(fragmentkey) :
            return self.__fragmentstop(fragmentkey)
        else :
            return (False, "not running")
            
    @log_functioncall
    def fragment_list(self) :
        return self.__listfragments()

    def __signalmaster(self) :
        try :
            p = xmlrpclib.ServerProxy(self.masterurl)
            p.register_resources(self.resource)
        except :
            pass # TODO what is thrown?

    def go(self) :
        #self.__signalmaster() # TODO what is the failure case, what exceptions are thrown, what should this throw?
        self.server.serve_forever()

    def fragment_done(self,fragmentkey, success, resultsfilename) : # TODO
        self.log.debug("%s %s" % (str(fragmentkey), "success" if success else "faliure"))
        #p = xmlrpclib.ServerProxy(self.masterurl)
        #p.fragment_complete(...)


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
""" %  (    sys.argv[0],                        \
            herzogdefaults.DEFAULT_WORKING_DIR, \
            herzogdefaults.DEFAULT_LOG_DIR,     \
            herzogdefaults.DEFAULT_KINSKI_PORT, \
            herzogdefaults.DEFAULT_HERZOG_HOST, \
            herzogdefaults.DEFAULT_HERZOG_PORT)

def error_msg(s) :
    print >> sys.stderr, "kinski: %s" % s
    sys.exit(-1)

def handleargs() :
    global options
    
    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:w:l:m:x:")

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

    handleargs()

    try :
        k = Kinski( options['portnumber'], \
                    options['workingdirectory'], \
                    options['logdirectory'], \
                    options['master'], \
                    options['masterportnumber'], \
                    verbose=options['verbose'])
        k.go()

    except KinskiInitialisationError, ie :
        error_msg(str(ie))
    except KeyboardInterrupt :
        sys.exit()

if __name__ == '__main__' :
    if os.name != 'posix' :
        print >> sys.stderr, "%s is only supported on posix systems at the moment"
    main()

