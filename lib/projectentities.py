import os
import string
import threading

class ProjectError(Exception) :
    pass

class Project :
    def __init__(self, name, path) :
        self.__validate_name(name)
        self.name = name
        self.path = path

        self.__find_fragments()

    def __validate_name(self,name) :
        chars = string.letters + string.digits + '-'
        if False in map(lambda x : x in chars, name) :
            raise "project names must only contain the following characters: %s" % chars

    def __find_fragments(self) : # TODO
        self.fragments = {}

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




