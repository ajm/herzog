import os
import re
import sys
import string
import threading
from glob import glob
from Queue import Queue

class ProjectError(Exception) :
    pass

class Project :
    def __init__(self, name, path) :
        self.__validate_name(name)
        fragments = self.__validate_path(path)

        self.processed_fragments = 0
        self.total_fragments = self.__get_number_of_fragments()

        self.processing_complete = False
    
        self.name = name
        self.path = path

        self.fragments = Queue()

    def __validate_name(self,name) :
        chars = string.letters + string.digits + '-'
        if False in map(lambda x : x in chars, name) :
            raise "project names must only contain the following characters: %s" % chars

    def __validate_path(self, path) :
        if not os.access(path, os.F_OK | os.R_OK | os.W_OK) :
            raise ProjectError("cannot access %s" % path)

        dir_re   = re.compile(".*c(\d+)$")
        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob(self.path + os.sep + "*"))

        number_chromosomes = len(listing)
        number_fragments = 0

        for dir in listing :
            inputfiles = glob(dir + os.sep + 'datain_*')
            number_fragments += len(inputfiles)

        if number_chromosomes == 0 or number_fragments == 0 :
            raise ProjectError("no input files found in %s" % path)

        return number_fragments

    def write_mega2_input(self, path) :
        abspath = path + os.path + "mega2_in.tmp"
        try :
            f = open(abspath, 'w')

        except IOError, ioe:
            raise ProjectError"could not open %s" % abspath) 

        print >> f, "1\n00\n0\n1\n2\n0\n0\n0" # '00' is the file extention
        f.close()

        return abspath

    def run_mega2(self, inputfile, path) :
        command = "cd %s ; mega2 < %s > /dev/null 2> /dev/null ; cd - > /dev/null 2> /dev/null" % (path, inputfile)
        os.system(command)
#        status,out = commands.getstatusoutput(command)
#        if status != 0 :
#            print >> sys.stderr, "mega2 did not run properly : return code %d" % status
#            sys.exit(-1)

        # check output file existance instead...
        missing = []
        files = {
            'sw2_pedigree.00' : 'PEDIGREE.DAT',
            'sw2_locus.00'    : 'LOCUS.DAT', 
            'sw2_pen.00'      : 'PEN.DAT',
            'sw2_batch.00'    : 'BATCH2.DAT',
            'sw2_map.00'      : 'MAP.DAT'
        }
        for oldfilename,newfilename in files :
            if not os.path.exists(path + os.sep + oldfilename) :
                missing.append(f)
            else :
                os.rename(path + os.sep + oldfilename, path + os.sep + newfilename)

        if len(missing) != 0 :
            raise ProjectError("%s not found after running mega2" % ','.join(missing))

    def __processing_complete(self) :
        self.processing_complete = True

    # TODO this must go into its own plugin or else in the kinski simwalk plugin...
    def process(self) :
        dir_re   = re.compile(".*c(\d+)$")
        input_re = re.compile("^datain_(\d+)\..*")

        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob(self.path + os.sep + "*"))
        mega2_input = self.write_mega2_input(path)

        for dir in listing :
            chromo = dir_re.match(dir).group(1)
            inputfiles = glob(dir + os.sep + 'datain_*')
            for f in inputfiles :
                dirname,filename = os.path.split(f)
                m = input_re.match(filename)
                if not m :
                    continue
                fragid = m.group(1)
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
                    run_mega2(mega2_input, fragdir)
                except ProjectError, pe :
                    # TODO report!
                    continue

                # TODO write file with project name, chromosome, fragment id, program,
                frag = (fragmentdir, ???) # TODO
                fragments.put( frag )

        self.__processing_complete()

    def next_fragment(self) : # TODO
        return self.fragments.get()

    def fragment_complete(self) : # TODO
        self.fragments.task_done() # i don't know if there are going to be an consumer threads - (could send an email!)

    def progress(self) : # TODO
#        if not self.processing_complete :
#            raise ProjectError("not all data has been preprocessed yet")

        return self.processed_fragments / self.total_fragments
        

    def __str__(self) :
        return self.name

class Job :
    def __init__(self, project, path, program) :
        self.project = project
        self.path = path
        self.program = program

class ProjectPool : # TODO use Queues...
    def __init__(self) :
        self.projects = {}

        self.resource_semaphore = threading.Semaphore(0)
        self.r_lock = threading.RLock()

    def __len__(self) :
        return len(self.projects)

    def add_project(self, project) :
        self.projects[project.name] = project

