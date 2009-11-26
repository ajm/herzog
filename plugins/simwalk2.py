from plugins import KinskiPlugin
import os, glob

class simwalk2(KinskiPlugin) :
    def __init__(self) :
        pass

    def inspect_input_files(self, path) :
        files   = ['PEDIGREE.DAT','LOCUS.DAT','PEN.DAT','BATCH2.DAT','MAP.DAT']
        listing = glob.glob(path + "/*")
        missing = []

        for f in files :
            if f not in listing :
                missing.append(f)

        if len(missing) != 0 :
            raise PluginError("input files missing: %s" % ','.join(missing))

    def inspect_system(self, resources) :
        pass

    def run(self, path) :
        os.system("cd %s ; simwalk2 > /dev/null 2> /dev/null ; cd -" % (path))
        
        score_files = filter(lambda x : x.startswith("SCORE"), glob.glob(path + "/*"))

        if len(score_files) == 1 :
            return path + "/" + score_files[0]

        raise PluginError("ambiguous results file, found %s" % ','.join(score_files))

