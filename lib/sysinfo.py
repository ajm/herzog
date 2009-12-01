import socket, commands, os

class Resource :
    def __init__(self):
        if os.name != 'posix' :
            raise NotImplemented

        self.populate()

    """
        __makedict turned data like this :

            key1 delimiter y
            key1 delimiter z
            key2 delimiter a
            key2 delimiter b


        into a dict of lists of strings like this :

        dict =   {
                    key = "key1"
                    value = [
                                "y"
                                "z"
                            ]
                    key = "key2"
                    value = [
                                "a"
                                "b"
                            ]
                }
    """
    def __makedict(self,filename, delimiter=None):
        info = {}
        f = open(filename)
        
        for line in f :
            data = line.strip().split(delimiter)

            if len(data) != 2 :
                continue

            key = data[0].strip()
            val = data[1].strip().split()

            if key not in info :
                info[key] = []

            info[key].append( val )
        f.close()

        return info

    def getmeminfo(self):
        data = self.__makedict('/proc/meminfo', delimiter=':')

        raminkB = data['MemTotal'][0]
        return int(raminkB[0]) / 1024

    def getcpuinfo(self):
        data = self.__makedict('/proc/cpuinfo', delimiter=':')

        cpuname  = ' '.join(data['model name'][0])
        cpuspeed = int(float(data['cpu MHz'][0][0]))
        cpucores = len(data['processor'])
        return cpuname,cpucores,cpuspeed

    def getdiskinfo(self):
        output = commands.getoutput('df -B1M')
        total = used = 0
        for line in output.split('\n') :
            data = line.split()
            if data[-1] == '/' :
                used  = int(data[2])
                total = int(data[3])
                break

        return total - used, total

    def populate(self):
        self.hostname = socket.gethostname()
        self.cpu, self.cpucores, self.cpuspeed  = self.getcpuinfo()
        self.ram                                = self.getmeminfo()
        self.__update_disk()

    def update(self):
        self.__update_disk()

    def __update_disk(self):
        self.disk_free, self.disk_total         = self.getdiskinfo()

    def __str__(self):
        return "hostname:\t%s\ncpu:\t%s\ncores:\t%d\nspeed:\t%d MHz\nram\t%d MB\ndisk:\t%d/%d MB" \
                % (self.hostname, self.cpu, self.cpucores, self.cpuspeed, self.ram, self.disk_free, self.disk_total)


if __name__ == '__main__' :
    r = resource()
    print str(r)

