import os
import re
import string
import threading
from glob import glob

class ProjectError(Exception) :
    pass

class Project :
    def __init__(self, name, path) :
        self.__validate_name(name)
        self.name = name
        self.path = path

    def __validate_name(self,name) :
        chars = string.letters + string.digits + '-'
        if False in map(lambda x : x in chars, name) :
            raise "project names must only contain the following characters: %s" % chars

    def write_mega2_input(self, path, extention) :
        pass

    def process(self) :
        regex = re.compile(".*c\d+$")
        listing = filter(lambda x : os.path.isdir(x) and regex.match(x), glob(self.path + os.sep + "*"))
        self.write_mega2_input(path, extention)

        # TODO

#        blocks = []
#
#        for chr in map(lambda x : "%02d" % x , range(1,25)) :
#            chrdir = "c%s" % chr
#            if os.path.exists(chrdir) and os.path.isdir(chrdir) :
# 
#                os.chdir(chrdir)
#
#                write_mega2_input(chr)
#
#                filelist = filter(lambda x : x.startswith("datain_") and x.endswith(chr), os.listdir('.'))
#                set  = max(map(lambda x : int(sp.match(x).group(1)), filelist))
#
#                for i in range(1, set + 1) :
#                    tmpdir = str(i)
#                    blocks.append((chrdir,tmpdir))
#                    
#                    try :
#                        shutil.rmtree(tmpdir) # XXX
#                    except :
#                        pass
#
#                    os.mkdir(tmpdir)
#                    os.chdir(tmpdir)
#
#                    shutil.copy("../datain_%d.%s" % (i, chr), "datain.%s" % chr)
#                    shutil.copy("../pedin_%d.%s" % (i, chr),  "pedin.%s" % chr)
#                    shutil.copy("../map_%d.%s" % (i, chr),    "map.%s" % chr)
#
#                    shutil.copy("../mega2_in.tmp", '.')
#
#                    run_mega2(chr)
#
#                    os.chdir("..")
#
#                os.chdir("..")
#
#        return blocks

    def next_fragment(self) : # TODO
        pass

    def fragment_complete(self) : # TODO
        pass

    def progress(self) : # TODO
        pass

    def __str__(self) :
        return self.name

class Job :
    def __init__(self, project, path, program) :
        self.project = project
        self.path = path
        self.program = program

class ProjectPool :
    def __init__(self) :
        self.projects = {}

        self.resource_semaphore = threading.Semaphore(0)
        self.r_lock = threading.RLock()

    def __len__(self) :
        return len(self.projects)

    def add_project(self, project) :
        self.projects[project.name] = project

