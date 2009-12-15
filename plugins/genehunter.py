import os
import re
import glob
import plugins
from plugins import KinskiPlugin, PluginError

# input:
#   datain_1.01
#   map_1.01
#   pedin_1.01
#   setup_1.01
#
# output:
#   gh_1.out
#   haplo.dump -> haplo_1.dump
#
# run:
#   cd c01
#   ghm < setup_1.01
#

class genehunter(KinskiPlugin) :

    def __init__(self) :
        pass

    def inspect_input_files(self, path) :
        missing = []

        for f in ['datain_*','map_*','pedin_*','setup_*'] :
            if len(glob.glob(path + os.sep + f)) == 0 :
                missing.append(f)

        if len(missing) != 0 :
            raise PluginError("input files missing: %s" % ','.join(missing))

    def inspect_system(self, resources) :
        pass

    def run(self, path) :
        setupfiles = filter(lambda x : (os.path.split(x)).startswith('setup'), glob.glob(path + os.sep + '*'))
        if len(setupfiles) > 1 :
            raise PluginError("more than one setup file, I don't know which to use: %s" % ','.join(setupfiles))

        os.system("cd %s ; ghm < %s > /dev/null 2> /dev/null ; cd - > /dev/null 2> /dev/null" % (path, setupfiles[0]))

        resultsfiles = filter(lambda x : (os.path.split(x)).startswith('gh_'), glob.glob(path + os.sep + '*'))

        if len(resultsfiles) == 0 :
            raise PluginError("no results file")
        elif len(resultsfiles) == 1 :
            return resultsfiles[0]
        else :
            raise PluginError("ambiguous results file, found %s" % ','.join(resultsfiles))

    def kill(self) :
        pass

    def process_all_input(self, projectname, path, queue, \
            increment_preprocessed_func, increment_processed_func, cancelled_func) :
        dir_re   = re.compile(".*c(\d+)$")
        input_re = re.compile("^datain_(\d+)\..*")

        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob.glob(path + os.sep + "*"))

        if len(listing) == 0 :
            raise PluginError("no chromosome directories to process in %s" % path)

        for dir in listing :
            chromo = dir_re.match(dir).group(1)
            inputfiles = glob.glob(dir + os.sep + 'datain_*')

            for f in inputfiles :
                if cancelled_func() :
                    return
                
                dirname,filename = os.path.split(f)
                m = input_re.match(filename)
                if not m :
                    continue
                fragid = m.group(1)

                if os.path.exists(dirname + os.sep + ("gh_%s.out" % fragid)) :
                    increment_processed_func()
                    continue

                input  = map(lambda x : fragdir + os.sep + (x % (fragid,chromo)), \
                        ['datain_%s.%s','map_%s.%s','pedin_%s.%s','setup_%s.%s'])
                output = dir + os.sep + ("gh_%s.out" % fragid)
                tmp = (input,output)

                queue.put( tmp )
                increment_preprocessed_func()
