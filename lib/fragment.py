import os, random, string

class FragmentError(Exception) :
    pass

class Fragment :
    def __init__(self, path, program, plugin):
        self.projectdirectory = path
        self.program = program
        self.plugin = plugin

        if not os.path.exists(self.projectdirectory) :
            raise FragmentError("%s does not exist" % self.projectdirectory)

    def run_plugin(self) :
        self.plugin.run(self.projectdirectory)

    def kill_plugin(self) :
        self.plugin.kill()

    @staticmethod
    def mk_tmp_directory(p) :
        # ensure the project directory exists...
        try :
            if not os.path.isdir(p) :
                os.mkdir(p)

        except OSError, ose :
            raise FragmentError(str(ose))

        chars = string.letters + string.digits

        # create a directory with a randomly generated name
        # to hold the temp input + output files
        while True :

            randomdir = ''.join(map(lambda x : chars[int(random.random() * len(chars))], range(8)))
            randomdir = p + os.sep + randomdir

            if not os.path.exists(randomdir) :
                try :
                    os.mkdir(randomdir)
                    os.chmod(randomdir, 0777)
                    
                except OSError, ose :
                    raise FragmentError(str(ose))

                break

        return randomdir

    def key(self) :
        return (self.projectdirectory, self.program)

    def __str__(self):
        return self.program + " @ " + self.projectdirectory

