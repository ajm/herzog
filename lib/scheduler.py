#import threading
from Queue import Queue
from resourceentities import *
from projectentities  import *

class Scheduler :
    def __init__(self, projects, resources, logger=None) :
        self.projects = projects    # queue of ProjectPool objects that each contain a queue
        self.resources = resources  # ResourcePool object that contains a queue of resource names, but returns a resource object
        self.log = logger

    def get_resource_job(self) :
        r = self.get_resource()
        j = self.get_job()

        return (r,j)

    def get_resource(self) :
        return self.resources.get_resource()

    def get_job(self) :
        raise NotImplemented

class FairScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class PriorityScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class FIFOScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)
        self.__next_project()

    def get_current_project(self) :
        return self.current_project.name

    def running(self, name) :
        return name == self.current_project.name

    def cancel_current_project(self) :
        self.cancelled = True

    def __next_project(self) :
        self.current_project = self.projects.next_project()
        self.cancelled = False

    def get_job(self) :
        j = None

        while j == None :
            j = self.current_project.next_fragment()

            if (not j) or self.cancelled :
                self.__next_project()

        return j

