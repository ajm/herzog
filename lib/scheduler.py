
class Scheduler :
    def __init__(self, projects, resources) :
        self.projects = projects
        self.resources = resources

class FairScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

class PriorityScheduler(Scheduler) :
    def __init__(self, projects, resources) :
        Scheduler.__init__(self, projects, resources)

