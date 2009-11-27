import os, glob
import plugins
import utils
import herzogdefaults
from plugins import KinskiPlugin, PluginError

class simwalk2(KinskiPlugin) :
    def __init__(self) :
        self.log = utils.get_logger('simwalk2_plugin', '.', herzogdefaults.KINSKI_LOG_FILENAME, verbose=True)

    def inspect_input_files(self, path) :
        files   = ['PEDIGREE.DAT','LOCUS.DAT','PEN.DAT','BATCH2.DAT','MAP.DAT']
        missing = plugins.plugin_helper_missing_files(path, files)

        if len(missing) != 0 :
            self.log.debug("simwalk2 files missing %s" % ','.join(missing))
            raise PluginError("input files missing: %s" % ','.join(missing))

    def inspect_system(self, resources) :
        pass

    def run(self, path) :
        self.log.debug("run start")

        os.system("cd %s ; simwalk2 &> /dev/null ; cd - &> /dev/null" % (path))
        
        score_files = filter(lambda x : os.path.basename(x).startswith("SCORE"), glob.glob(path + "/*"))
        
        if len(score_files) == 0 :
            raise PluginError("no results file")
        elif len(score_files) == 1 :
            return path + os.sep + score_files[0]
        else :
            raise PluginError("ambiguous results file, found %s" % ','.join(score_files))

    def kill(self) :
        pass

