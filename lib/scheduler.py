#import threading
from Queue import Queue
from resourceentities import *
from projectentities  import *

class Job :
    def __init__(self, project, path, program) :
        self.project = project
        self.path = path
        self.program = program

class Scheduler :
    def __init__(self, projects, resources, logger=None) :
        self.projects = projects # queue of ProjectPool objects that each contain a queue
        self.resources = resources # ResourcePool object that contains a queue of resource names, but returns a resource object
        self.log = logger

#        self.last_project = None

#        self.sema = threading.Semaphore(0)
#        self.r_lock = threading.RLock()

    def get_resource_job(self) :
        r = self.resources.get_resource()
        j = self.next_job()

        return (r,j)

#    def add_project(self, p) :
#        rel = (len(self.projects) == 0)
#        self.projects.append(p)
#
#        if rel :
#            self.sema.release()

    def next_job(self) :
        raise NotImplemented

#    def get_next_job(self) :
#        if len(self.projects) == 0 :
#            self.sema.acquire()
#
#        job = self.next_job()
#        self.last_project = job.project
#
#        return job

class FairScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class PriorityScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class FIFOScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)
        self.current_project = None

    def next_job(self) : # TODO add these calls to other classes
        if not self.current_project or self.current_project.complete() :
            self.current_project = self.projects.next_project()

        return self.current_project.next_fragment()

