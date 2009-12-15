import os
import re
import glob
import shutil
import plugins
from plugins import KinskiPlugin, PluginError

# input:
#   PEDIGREE.DAT
#   LOCUS.DAT
#   PEN.DAT
#   BATCH2.DAT
#   MAP.DAT
#
# output:
#   SCORE-01.ALL
# 
# run :
#   cd c01
#   simwalk2
#

class simwalk2(KinskiPlugin) :
    def __init__(self) :
        pass

    def inspect_input_files(self, path) :
        files   = ['PEDIGREE.DAT','LOCUS.DAT','PEN.DAT','BATCH2.DAT','MAP.DAT']
        missing = plugins.plugin_helper_missing_files(path, files)

        if len(missing) != 0 :
            raise PluginError("input files missing: %s" % ','.join(missing))

    def inspect_system(self, resources) :
        pass

    def run(self, path) :

        os.system("cd %s ; simwalk2 ; cd - > /dev/null" % (path))
        
        score_files = filter(lambda x : re.match("^SCORE.*\.ALL$", os.path.basename(x)) , glob.glob(path + os.sep + "*"))
        
        if len(score_files) == 0 :
            raise PluginError("no results file")
        elif len(score_files) == 1 :
            return score_files[0]
        else :
            raise PluginError("ambiguous results file, found %s" % ','.join(score_files))

    def kill(self) :
        pass

    def __write_mega2_input(self, path) :
        abspath = path + os.sep + "mega2_in.tmp"
        try :
            f = open(abspath, 'w')

        except IOError, ioe:
            raise PluginError("could not open %s" % abspath)

        print >> f, "1\n00\n0\n1\n2\n0\n0\n0" # '00' is the file extention
        f.close()

        return abspath

    def __run_mega2(self, inputfile, path, chromo) :
        command = "cd %s ; mega2 < %s > /dev/null 2> /dev/null ; cd - > /dev/null 2> /dev/null" % (path, inputfile)
        os.system(command)

        # status from os.system is borked due to multiple commands
        # but i need to do it that way to chdir in a thread...
        # check output file existance instead...
        missing = []
        files = {
            'sw2_pedigree.%s' % chromo : 'PEDIGREE.DAT',
            'sw2_locus.%s' % chromo    : 'LOCUS.DAT',
            'sw2_pen.%s' % chromo      : 'PEN.DAT',
            'sw2_batch.%s' % chromo    : 'BATCH2.DAT',
            'sw2_map.%s' % chromo      : 'MAP.DAT'
        }
        for oldfilename,newfilename in files.items() :
            if not os.path.exists(path + os.sep + oldfilename) :
                missing.append(oldfilename)
            else :
                os.rename(path + os.sep + oldfilename, path + os.sep + newfilename)

        if len(missing) != 0 :
            raise PluginError("%s not found after running mega2" % ','.join(missing))

    def __write_summary(self, fragdir, project, program, chromosome, fragment) :
        f = open(fragdir + os.sep + "SUMMARY.DAT", 'w')
        print >> f, "%s %s %s %s" % (project, program, chromosome, fragment)
        f.close()

    def process_all_input(self, projectname, path, queue, increment_preprocessed_func, increment_processed_func, cancelled_func) :
        dir_re   = re.compile(".*c(\d+)$")
        input_re = re.compile("^datain_(\d+)\..*")

        listing = filter(lambda x : os.path.isdir(x) and dir_re.match(x), glob.glob(path + os.sep + "*"))
        mega2_input = self.__write_mega2_input(path)

        if len(listing) == 0 :
            raise PluginError("no chromosome directories to process in %s" % path)

        # for each chromosome directory
        for dir in listing :
            chromo = dir_re.match(dir).group(1)
            inputfiles = glob.glob(dir + os.sep + 'datain_*')

            # for each input file
            for f in inputfiles :

                # fast fail
                if cancelled_func() :
                    return

                dirname,filename = os.path.split(f)
                m = input_re.match(filename)
                if not m :
                    continue
                fragid = m.group(1)

                # already been processed... skip
                if os.path.exists(dirname + os.sep + ("SCORE-%s_%s.ALL" % (chromo, fragid))) :
                    increment_processed_func()
                    continue

                # if temp fragment dir exists delete it
                fragdir = dirname + os.sep + fragid
                if os.path.exists(fragdir) :
                    try :
                        shutil.rmtree(fragdir)

                    except :
                        pass

                # make fragment dir
                try :
                    os.mkdir(fragdir)

                except OSError, ose :
                    self.log.error(str(ose))
                    continue

                # copy data files in...
                shutil.copy(dir + os.sep + ("datain_%s.%s" % (fragid,chromo)),  fragdir + os.sep + "datain.00")
                shutil.copy(dir + os.sep + ("pedin_%s.%s" % (fragid,chromo)),   fragdir + os.sep + "pedin.00")
                shutil.copy(dir + os.sep + ("map_%s.%s" % (fragid,chromo)),     fragdir + os.sep + "map.00")

                # run mega2 to convert input files
                try :
                    self.__run_mega2(mega2_input, fragdir, chromo)
                except PluginError, pe :
                    # TODO report! or log in some way
                    continue

                # write summary
                self.__write_summary(fragdir, projectname, "simwalk2", chromo, fragid) 

                # make job object, TODO define this as a class so it is unambiguous...
                input  = map(lambda x : fragdir + os.sep + x, \
                        ['PEDIGREE.DAT','LOCUS.DAT','PEN.DAT','BATCH2.DAT','MAP.DAT','SUMMARY.DAT'])
                output = dir + os.sep + ("SCORE-%s_%s.ALL" % (chromo,fragid))
                tmp = (input,output)

                queue.put( tmp )
                increment_preprocessed_func()

