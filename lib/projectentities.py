import os
import re
import sys
import time
import string
import shutil
import threading
import plugins
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
    ERROR           = 6

    def __init__(self, name, path, program, logger) :
        self.__validate_name(name)
        numfragments = self.__validate_path(path)

        self.name       = name
        self.path       = path
        self.program    = program
        self.log        = logger

        self.preprocessed_fragments     = 0
        self.processed_fragments        = 0
        self.total_fragments            = numfragments

        self.fragments = Queue()
        self.state = Project.STARTED

        self.start_time = -1
        self.map = {}

        # XXX TODO : find plugin for program + add throw exception if not found...
        try :
            self.plugin = plugins.get_plugin(self.program)

        except plugins.PluginError, pe :
            raise ProjectError(str(pe))

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

    def started(self) :
        return self.start_time != -1

    def cancelled(self) :
        return self.state == Project.CANCELLED

    def finished(self) :
        return self.processed_fragments == self.total_fragments

    def increment_preprocessed(self) :
        self.preprocessed_fragments += 1

    def increment_processed(self) :
        self.processed_fragments += 1

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

    def process(self) :
        try :
            self.plugin.process_all_input(self.name, self.path, self.fragments, \
                self.increment_preprocessed, self.increment_processed, self.cancelled)
            self.__preprocessing_complete()
        except plugins.PluginError, pe :
            self.log.error(str(pe))
            self.state = Project.ERROR

    def mapping_put(self, x, y) :
        self.map[x] = y

    def mapping_get(self, x) :
        tmp = self.map[x]
        del self.map[x]
        return tmp

    def next_fragment(self) :
        if self.state == Project.RUNNING and self.fragments.empty() :
            #self.log.debug("project: running, but no fragments...")
            return None

        if self.state == Project.COMPLETED or self.state == Project.CANCELLED :
            #self.log.debug("project: completed or cancelled")
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
        elif self.state == Project.ERROR :
            return ('error', -1.0)
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

