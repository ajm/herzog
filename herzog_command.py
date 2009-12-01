#!/usr/bin/env python
import sys
import os
import xmlrpclib

from etc import herzogdefaults


def usage() :
    print >> sys.stderr, \
"""Usage %s

        add project_name directory_name
            Add a project called 'project_name' to the queue, 
            the input files of which are contained in 'directory_name'.

        rm  project_name
            Remove project 'project_name' from the queue.

        pause project_name
            Do not run anything for project 'project_name' for the time
            being.

        resume project_name
            Resume a previously halted project.

        progress [project_name]
            Print out the percentage complete of running projects.

        when [project_name]
            Give an estimate of how long a project has to complete.

        schedule schedule_name
            Change scheduler behaviour.

        help
            Print this message.
""" % sys.argv[0]
    sys.exit(-1)

def getproxy() :
    return xmlrpclib.ServerProxy("http://%s:%d" % (socket.gethostbyname(), herzogdefaults.DEFAULT_HERZOG_PORT))

def add_project(args) : 
    p = getproxy()
    successful,msg = p.project_add(args[0], args[1])

    if not successful :
        print >> sys.stderr, "herzog: %s" % msg

def rm_project(args) : 
    p = getproxy()
    successful,msg = p.project_remove(args[0])

    if not successful :
        print >> sys.stderr, "herzog: %s" % msg

def pause_project(args) : 
    p = getproxy()
    successful,msg = p.project_pause(args[0])

    if not successful :
        print >> sys.stderr, "herzog: %s" % msg

def resume_project(args) : 
    p = getproxy()
    successful,msg = p.project_pause(args[0])

    if not successful :
        print >> sys.stderr, "herzog: %s" % msg

def progress(args) : # TODO
    pass

def estimate_end(args) : # TODO
    pass

def scheduler_policy(args) : 
    raise NotImplemented

def main() :
    if len(sys.argv) < 2 :
        usage()

    command = sys.argv[1]
    args = sys.argv[2:]

    if command in ['-h','help'] :
        usage()

    elif command == 'add' and len(args) == 2 :
        add_project(args)

    elif command == 'rm' and len(args) == 1 :
        rm_project(args)

    elif command == 'pause' and len(args) == 1 :
        pause_project(args)

    elif command == 'resume' and len(args) == 1 :
        resume_project(args)

    elif command == 'progress' and len(args) in [0,1] :
        progress(args)

    elif command == 'when' and len(args) in [0,1] :
        estimate_end(args)

    elif command == 'schedule' and len(args) == 1 :
        scheduler_policy(args)

    else :
        usage()

if __name__ == '__main__' :
    main()

