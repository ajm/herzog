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

    STARTED         = 0
    PREPROCESSING   = 1
    READY           = 2
    RUNNING         = 3     
    COMPLETED       = 4
    CANCELLED       = 5

    def __init__(self, name, path, program) :
        self.__validate_name(name)
        numfragments = self.__validate_path(path)

        self.name       = name
        self.path       = path
        self.program    = program

        self.preprocessed_fragments     = 0
        self.processed_fragments        = 0
        self.total_fragments            = numfragments

        self.fragments = Queue()
        self.state = Project.STARTED

        self.start_time = -1
        self.map = {}

    def started(self) :
        return self.start_time != -1

    def finished(self) :
        return self.processed_fragments == self.total_fragments

    def __validate_name(self,name) :
        chars = string.letters + string.digits + '-'
        if False in map(lambda x : x in chars, name) :
            raise ProjectError("project names must only contain the following characters: %s" % chars)

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

    def __preprocessing_complete(self) :
        if self.state == Project.CANCELLED :
            return
        elif not self.started() :
            self.state = Project.READY
        elif self.finished() :
            self.state = Project.COMPLETED
        else :
            self.state = Project.RUNNING

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
                    self.processed_fragments += 1
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

                self.write_summary(fragdir, self.name, chromo, fragid) # TODO: is this cool?

                tmp = (fragdir, dir + os.sep + ("SCORE-%s_%s.ALL" % (chromo,fragid)))
                # TODO write file with project name, chromosome, fragment id, program,
                self.fragments.put( tmp )
                self.preprocessed_fragments += 1

        self.__preprocessing_complete()

    def write_summary(self, fragdir, project, chromosome, fragment) :
        f = open(fragdir + os.sep + "SUMMARY.DAT", 'w')
        print >> f, "%s %s %s" % (project, chromosome, fragment)
        f.close()

    def mapping_put(self, x, y) :
        self.map[x] = y

    def mapping_get(self, x) :
        tmp = self.map[x]
        del self.map[x]
        return tmp

    def next_fragment(self) :
        if self.state == Project.RUNNING and self.fragments.empty() :
            print "\n\npro: running, but no fragments...\n\n"
            return None

        if self.state == Project.COMPLETED or self.state == Project.CANCELLED :
            print "\n\npro: completed or cancelled\n\n"
            return None

        fragdir,resultsfile = self.fragments.get()

        return Job(self.name, fragdir, self.program, resultsfile)

    def finished(self) :
        return self.processed_fragments == self.total_fragments

    def fragment_complete(self) :
        self.processed_fragments += 1
        if self.state == Project.RUNNING and self.finished() :
            self.state = Project.COMPLETED

        self.fragments.task_done() 
        # i don't know if there are going to be an consumer threads - (could send an email!)

    def progress(self) : 
        prog = (self.processed_fragments / float(self.total_fragments)) * 100.0

        if self.state == Project.PREPROCESSING :
            return ('preprocessing',  (self.preprocessed_fragments / float(self.total_fragments)) * 100.0)
        elif self.state == Project.READY :
            return ('ready', prog)
        elif self.state == Project.RUNNING :
            return ('running', prog)
        elif self.state == Project.COMPLETED :
            return ('complete', prog)
        elif self.state == Project.CANCELLED :
            return ('cancelled', prog)
        else :
            return ('unknown', -1.0)

    def start(self) :
        self.start_time = time.time()

        if self.state == Project.READY :
            self.state = Project.RUNNING

    def cancel(self) :
        self.state = Project.CANCELLED

    def pause(self) :
        if self.state in [Project.READY, Project.RUNNING]:
            self.state = Project.CANCELLED
        else :
            raise ProjectError("only 'ready' or 'running' projects can be paused")

    def resume(self) :
        if self.state == Project.CANCELLED :
            self.state = Project.READY
        else :
            raise ProjectError("only 'cancelled' projects can be resumed")
        
    def __str__(self) :
        return self.name

class Job :
    def __init__(self, project, path, program, resultsfile) :
        self.project = project
        self.path = path
        self.program = program
        self.resultsfile = resultsfile

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

    def pause(self, name) : 
        # don't bother removing from the queue, state of project will take care of that...
        try :
            p = self.projects[name]
            p.pause()
        except KeyError, ke :
            raise ProjectError("'%s' does not exist" % name)

    def resume(self, name) :
        try :
            p = self.projects[name]
            p.resume()
            self.project_queue.put(p)
        except KeyError, ke :
            raise ProjectError("'%s' does not exist" % name)

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

