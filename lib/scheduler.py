import threading
from resourceentities import *
from projectentities  import *

class Job :
    def __init__(self, project, path, program) :
        self.project = project
        self.path = path
        self.program = program

class Scheduler :
    def __init__(self, projects, resources, logger=None) :
        self.projects = projects
        self.resources = resources
        self.log = logger

        self.last_project = None

        self.sema = threading.Semaphore(0)
        self.r_lock = threading.RLock()

    def get_resource_job(self) :
        r = self.resources.get_resource()
        j = self.get_next_job()

        return (r,j)

    def add_project(self, p) :
        rel = (len(self.projects) == 0)
        self.projects.append(p)

        if rel :
            self.sema.release()

    def next_job(self) :
        raise NotImplemented

    def get_next_job(self) :
        if len(self.projects) == 0 :
            self.sema.acquire()

        job = self.next_job()
        self.last_project = job.project

        return job

class FairScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class PriorityScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class FIFOScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

    def next_job(self) :
        pass # TODO

