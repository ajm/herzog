import os
import re
import glob
import plugins
from plugins import KinskiPlugin, PluginError

# input:
#   allegro.in
#   datain.01
#   map.01
#   pedin.01
#
# output:
#   .*\.out
#   from file allegro.in, the last argument on a line starting with MODEL
#
# run:
#   cd c01
#   allegro allegro.in
#

class genehunter(KinskiPlugin) :

    def __init__(self) :
        pass

    def inspect_input_files(self, path) :
        missing = []

        for f in ['allegro.in','datain.*','pedin.*','map.*'] :
            if len(glob.glob(path + os.sep + f)) == 0 :
                missing.append(f)

        if len(missing) != 0 :
            raise PluginError("input files missing: %s" % ','.join(missing))

    def inspect_system(self, resources) :
        pass

    def run(self, path) :

        os.system("cd %s ; allegro allegro.in > /dev/null 2> /dev/null ; cd - > /dev/null 2> /dev/null" % path)

        resultsfiles = glob.glob(path + os.sep + '*.out')

        f = open(path + os.sep + 'allegro.in')
        for line in f :
            tmp = line.strip()
            if not tmp.startswith("MODEL") :
                continue
            resultsfiles.append(path + os.sep + tmp.split()[-1])
        f.close()

        if len(resultsfiles) == 0 :
            raise PluginError("no results files")

        return resultsfiles

    def kill(self) :
        pass

    def write_allegro_input(directory, chr) :
        f = open(directory + os.sep + 'allegro.in', 'w')
        print >> f, "PREFILE pedin.%s" % chr
        print >> f, "DATFILE datain.%s" % chr
        print >> f, "MODEL mpt par het param_mpt.%s" % chr
        print >> f, "MODEL mpt lin all equal linall_mpt.%s"
        print >> f, "HAPLOTYPE"
        print >> f, "MAXMEMORY 1200"
        f.close()

    def process_all_input(self, projectname, path, queue, \
            increment_preprocessed_func, increment_processed_func, cancelled_func) :
        dir_re   = re.compile(".*c(\d+)$")
        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob.glob(path + os.sep + "*"))

        if len(listing) == 0 :
            raise PluginError("no chromosome directories to process in %s" % path)

        for dir in listing :
            chromo = dir_re.match(dir).group(1)
            inputfiles = glob.glob(dir + os.sep + 'datain.*')

            for f in inputfiles :
                if cancelled_func() :
                    return

                dirname,filename = os.path.split(f)
                write_allegro_input(dirname, chromo)
                
                if len(glob.glob(dirname + os.sep + "*mpt*")) != 0 :
                    increment_processed_func()
                    continue

                input  = map(lambda x : dirname + os.sep + (x % (chromo)), \
                        ['datain.%s','map.%s','pedin.%s'])
                output = dirname
                tmp = (input,output)

                queue.put( tmp )
                increment_preprocessed_func()

