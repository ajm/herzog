import os, glob, re
import plugins
from plugins import KinskiPlugin, PluginError

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

