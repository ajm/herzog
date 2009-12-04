from Queue import Queue

class ResourcePool :
    def __init__(self) :
        self.resources = {}
        self.available = Queue()

    def add_host_resource(self, r) :
        self.resources[ r['hostname'] ] = r
        for i in range( r['cpucores'] ) :
            self.add_core_resource( r['hostname'] )

    def add_core_resource(self, hostname) :
        self.available.put(hostname)

    def get_resource(self) :
        return self.resources[ self.available.get() ] 

    def details(self) :
        return self.resources.values()

    def __str__(self) :
        s = "%10s\t%d cores @ %.2f GHz"
        ret = ""
        for r in self.resources.values() :
            ret += (s % (r['hostname'], r['cpucores'], (r['cpuspeed'] / 1000.0)))

        return ret

