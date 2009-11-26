#!/usr/bin/env python
import sys, getopt, threading, os, string, random
import logging, logging.handlers
from SimpleXMLRPCServer import SimpleXMLRPCServer

from sysinfo import resource
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
    def __init__(self, pluginobj) :
        self.program = pluginobj

    def run() :
        self.program.run()

class Fragment : # makes 'fragment token' redundant?
    def __init__(self,path,program):
        self.path = path
        self.program = program
        # do other stuff...

    def token(self) :
        pass

    def __makesubdir(self) :
        pass

class FragmentToken :
    def __init__(self, hostname, path, program) :
        self.hostname = hostname
        self.path = path
        self.program = program
        self.token = (hostname,path,program)

    def __str__(self) :
        return "%s:%s:%s" % self.token

class KinskiInitialisationError(Exception) :
    pass

class Kinski :
    
    version = '0.1'

    def __init__(self, portnumber, baseworkingdir, logdir='/var/log', plugindir='plugins', verbose=False) :
        self.setuplogging(logdir, verbose)
        self.setupworkingdirectory(baseworkingdir)
        self.resource = resource()
        self.workers = {}
        random.seed()

        plugins.init_plugins(plugindir)
        
        self.server = SimpleXMLRPCServer((self.resource.hostname, portnumber))
        
        self.server.register_function(self.resources_list,  'resources_list')
        self.server.register_function(self.fragment_prep,   'fragment_prep' )
        self.server.register_function(self.fragment_start,  'fragment_start')
        self.server.register_function(self.fragment_stop,   'fragment_stop' )
        self.server.register_function(self.fragment_list,   'fragment_list' )
        self.server.register_introspection_functions()

        self.log.debug("initialised")

    def name(self):
        return self.__class__.__name__.lower()

    def setuplogging(self, logdir, verbose) :
        if not os.access(logdir, os.F_OK | os.R_OK | os.W_OK) :
            raise KinskiInitialisationError("log directory %s does not exist or I do not have read/write permission" % logdir)

        self.logname = "%s/%s.log" % (logdir, self.name())
        self.log = logging.getLogger(self.name())
        self.log.setLevel(logging.DEBUG)

        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG if verbose else logging.ERROR)
        console.setFormatter(logging.Formatter("%(name)s:%(message)s"))
        
        logfile = logging.handlers.RotatingFileHandler(self.logname, maxBytes=2**20, backupCount=5)
        logfile.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

        self.log.addHandler(console)
        self.log.addHandler(logfile)

    def setupworkingdirectory(self, wd) :
        if not os.access(wd, os.F_OK | os.R_OK | os.W_OK) :
            raise KinskiInitialisationError("working directory %s does not exist or I do not have read/write permission" % wd)

        self.workingdir = wd

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
    def fragment_prep(self, project) :
        if project.count('/') != 0 :
            return (False, "%s is not a suitable project name because it contains '/'")

        projectdirectory = self.workingdir + "/" + project

        try :
            if not os.path.isdir(projectdirectory) :
                os.mkdir(projectdirectory)
        except OSError, ose :
            return (False, "%s : when creating project directory" % str(ose))

        chars = string.letters + string.digits
        randomdir = None

        while True :
            randomdir = projectdirectory + "/" + ''.join(map(lambda x : chars[int(random.random() * len(chars))], range(8)))
            if not os.path.exists(randomdir) :
                try :
                    os.mkdir(randomdir)
                except OSError, ose :
                    return (False, "%s : when creating project subdirectory" % str(ose))
                break

        return (True, randomdir)

    @log_functioncall
    def fragment_start(self, path, program) :
        try :
            p = plugins.get_plugin(program)

            p.inspect_input_files(path)
            p.inspect_system(self.resources)

        except plugins.PluginError, pe :
            return (False, str(pe))

        wt = WorkerThread(p) # and other stuff???
        wt.start()
        
        self.workers.append(wt) # XXX NO!, use a dictionary, with some kind of 'token' as the key, eg: (hostname,path) so i can stop it easily!
        
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
    print >> sys.stderr, "Usage: %s [-vh] [-pNUM] [-dDIR] [-lDIR]\n\t-v\tverbose\n\t-h\tprint this message\n\t-p\tport number (default: 8000)\n\t-d\tworking directory (default: /tmp)\n\t-l\tlog directory (default: /var/log)\n" % sys.argv[0]

def error_msg(s) :
    print >> sys.stderr, "kinski: %s" % s
    sys.exit(-1)

def handleargs() :
    global options
    
    try :
        opts,args = getopt.getopt(sys.argv[1:], "vhp:d:l:")
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
        elif o == "-d" :
            if not os.path.exists(a) :
                error_msg("%s does not exist" % a)

            options['workingdirectory'] = a
        elif o == "-l" :
            if not os.path.exists(a) :
                error_msg("%s does not exist" % a)

            options['logdirectory'] = a


def main() :
    global options

    options['verbose'] = False
    options['portnumber'] = 8000
    options['workingdirectory'] = '/tmp'
    options['logdirectory'] = '/var/log'

    handleargs()

    try :
        k = Kinski(options['portnumber'], options['workingdirectory'], logdir=options['logdirectory'], verbose=options['verbose'])
        k.go()
    except KinskiInitialisationError, ie :
        error_msg(str(ie))
    except KeyboardInterrupt :
        sys.exit()

if __name__ == '__main__' :
    main()

