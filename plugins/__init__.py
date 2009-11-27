import sys, os

class KinskiPlugin(object):
    def inspect_input_files(self, path) :
        raise NotImplemented

    def inspect_system(self, resources) :
        raise NotImplemented

    def run(self, path) :
        raise NotImplemented

    def kill(self) :
        raise NotImplemented

class PluginError(Exception) :
    pass

def init_plugins(pluginpath='plugins'):
    if not pluginpath in sys.path :
        sys.path.insert(0, pluginpath)

def get_plugin(pluginname) :
    try :
        __import__(pluginname, None, None, [''])

    except ImportError :
        raise PluginError("%s does not exist" % pluginname)

    for p in KinskiPlugin.__subclasses__() :
        if p.__name__ == pluginname :
            return p()

    # could this ever happen?
    raise PluginError("%s does not seem to exist despite successful import" % pluginname)

def plugin_helper_missing_files(path, files) :
    missing = []

    for f in files :
        if not os.path.exists(path + os.sep + f) :
            missing.append(f)

    return missing

