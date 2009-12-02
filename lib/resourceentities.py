import threading

class ResourcePool :
    def __init__(self) :
        self.resources = {}
        self.available = []

        self.resource_semaphore = threading.Semaphore(0)
        self.r_lock = threading.RLock()

    def add_host_resource(self, r) :
        self.r_lock.acquire()

        self.resources[ r['hostname'] ] = r
        for i in range( r['cpucores'] ) :
            self.add_core_resource(r['hostname'])

        self.r_lock.release()

    def add_core_resource(self, hostname) :
        self.r_lock.acquire()

        self.available.append(hostname)
        self.resource_semaphore.release()

        self.r_lock.release()

    def get_resource(self) :
        self.resource_semaphore.acquire()

        self.r_lock.acquire()
        resource = self.resources[ self.available_resources.pop() ] 
        self.r_lock.release()

        return resource

