import os
import re
import sys
import time
import string
import shutil
import threading
from glob import glob
from Queue import Queue

class ProjectError(Exception) :
    pass

class Project :

    STARTED = 0
    PREPROCESSING = 1
    PREPROCESSED = 2
    RUNNING = 3     # unused
    COMPLETED = 4   # unused
    CANCELLED = 5

    def __init__(self, name, path, program) :
        self.__validate_name(name)
        numfragments = self.__validate_path(path)

        self.preprocessed_fragments     = 0
        self.processed_fragments        = 0
        self.total_fragments            = numfragments

#        self.processing_complete = False
    
        self.name = name
        self.path = path
        self.program = program
        self.start_time = 0

        self.fragments = Queue()

        self.state = Project.STARTED

    def start(self) :
        self.start_time = time.time()

    def __validate_name(self,name) :
        chars = string.letters + string.digits + '-'
        if False in map(lambda x : x in chars, name) :
            raise "project names must only contain the following characters: %s" % chars

    def __validate_path(self, path) :
        if not os.access(path, os.F_OK | os.R_OK | os.W_OK) :
            raise ProjectError("cannot access %s" % path)

        dir_re   = re.compile(".*c(\d+)$")
        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob(path + os.sep + "*"))

        number_chromosomes = len(listing)
        number_fragments = 0

        for dir in listing :
            inputfiles = glob(dir + os.sep + 'datain_*')
            number_fragments += len(inputfiles)

        if number_chromosomes == 0 or number_fragments == 0 :
            raise ProjectError("no input files found in %s" % path)

        return number_fragments

    def write_mega2_input(self, path) :
        abspath = path + os.sep + "mega2_in.tmp"
        try :
            f = open(abspath, 'w')

        except IOError, ioe:
            raise ProjectError("could not open %s" % abspath) 

        print >> f, "1\n00\n0\n1\n2\n0\n0\n0" # '00' is the file extention
        f.close()

        return abspath

    def run_mega2(self, inputfile, path, chromo) :
        command = "cd %s ; mega2 < %s > /dev/null 2> /dev/null ; cd - > /dev/null 2> /dev/null" % (path, inputfile)
        os.system(command)
#        status,out = commands.getstatusoutput(command)
#        if status != 0 :
#            print >> sys.stderr, "mega2 did not run properly : return code %d" % status
#            sys.exit(-1)

        # status from os.system is borked due to multiple commands
        # but i need to do it that way to chdir in a thread...
        # check output file existance instead...
        missing = []
        files = {
            'sw2_pedigree.%s' % chromo : 'PEDIGREE.DAT',
            'sw2_locus.%s' % chromo    : 'LOCUS.DAT', 
            'sw2_pen.%s' % chromo      : 'PEN.DAT',
            'sw2_batch.%s' % chromo    : 'BATCH2.DAT',
            'sw2_map.%s' % chromo      : 'MAP.DAT'
        }
        for oldfilename,newfilename in files.items() :
            if not os.path.exists(path + os.sep + oldfilename) :
                missing.append(oldfilename)
            else :
                os.rename(path + os.sep + oldfilename, path + os.sep + newfilename)

        if len(missing) != 0 :
            raise ProjectError("%s not found after running mega2" % ','.join(missing))

    def __processing_complete(self) :
        self.state = Project.PREPROCESSED
#        self.processing_complete = True

    def process_background(self) :
        self.state = Project.PREPROCESSING
        t = threading.Thread(target=self.process)
        t.start()

    # TODO this must go into its own plugin or else in the kinski simwalk plugin...
    def process(self) :
        dir_re   = re.compile(".*c(\d+)$")
        input_re = re.compile("^datain_(\d+)\..*")

        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob(self.path + os.sep + "*"))
        mega2_input = self.write_mega2_input(self.path)

        for dir in listing :
            chromo = dir_re.match(dir).group(1)
            inputfiles = glob(dir + os.sep + 'datain_*')

            for f in inputfiles :

                if self.state == Project.CANCELLED :
                    return

                dirname,filename = os.path.split(f)
                m = input_re.match(filename)
                if not m :
                    continue
                fragid = m.group(1)

                if os.path.exists(dirname + os.sep + ("SCORE-%s_%s.ALL" % (chromo, fragid))) :
                    continue

                fragdir = dirname + os.sep + fragid
                if os.path.exists(fragdir) :
                    try :
                        shutil.rmtree(fragdir)

                    except :
                        pass
                try :
                    os.mkdir(fragdir)

                except OSError, ose :
                    self.log.error(str(ose))
                    continue
                
                shutil.copy(dir + os.sep + ("datain_%s.%s" % (fragid,chromo)),  fragdir + os.sep + "datain.00")
                shutil.copy(dir + os.sep + ("pedin_%s.%s" % (fragid,chromo)),   fragdir + os.sep + "pedin.00")
                shutil.copy(dir + os.sep + ("map_%s.%s" % (fragid,chromo)),     fragdir + os.sep + "map.00")
                
                try :                
                    self.run_mega2(mega2_input, fragdir, chromo)
                except ProjectError, pe :
                    # TODO report! or log in some way
                    continue

                # TODO write file with project name, chromosome, fragment id, program,
                self.fragments.put( fragdir )
                self.preprocessed_fragments += 1

        self.__processing_complete()

    def next_fragment(self) :
#        if self.processing_complete and self.fragments.empty() :
        if self.state == Project.PREPROCESSED and self.fragments.empty() :
            return None

        frag = self.fragments.get()

        return Job(self.name, frag, self.program)

    def finished(self) :
        return self.processed_fragments == self.total_fragments

    def fragment_complete(self) :
        self.processed_fragments += 1
        self.fragments.task_done() 
        # i don't know if there are going to be an consumer threads - (could send an email!)

    def progress(self) : 
#        return (self.processed_fragments / float(self.total_fragments)) * 100.0
        if self.state == Project.PREPROCESSING :
            return ('preprocessing',    (self.preprocessed_fragments / float(self.total_fragments)) * 100.0)
        elif self.state == Project.PREPROCESSED :
            return ('running',          (self.processed_fragments / float(self.total_fragments)) * 100.0)
        else :
            return ('unknown', -1.0)

    def cancel(self) :
        self.state = Project.CANCELLED
        
    def __str__(self) :
        return self.name

class Job :
    def __init__(self, project, path, program) :
        self.project = project
        self.path = path
        self.program = program

    def __str__(self) :
        return "%s : %s : %s" % (self.project, self.program, self.path)

class ProjectPool : 
    def __init__(self) :
        self.projects = {}
        self.project_queue = Queue()

    def __len__(self) :
        return len(self.projects)

    def exists(self,name) :
        return name in self.projects

    def next_project(self) :
        p = self.project_queue.get()
        p.start()
        return p

    def remove(self,name) :
        self.projects[name].cancel()
        del self.projects[name]

    def cleanup(self,name) :
        self.remove(name)

    def put_project(self, project) :
        self.projects[project.name] = project
        self.project_queue.put(project)
    
    def get_project(self, name) :
        try :
            return self.projects[name]
        except KeyError, ke :
            raise ProjectError("%s is not an active project" % name)

    def get_project_names(self) :
        return self.projects.keys()

