#!/usr/bin/env python
import sys
import os
import socket
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

def main() :
    if len(sys.argv) < 2 :
        usage()

    p = xmlrpclib.ServerProxy("http://%s:%d" % (socket.gethostname(), herzogdefaults.DEFAULT_HERZOG_PORT))

    command = sys.argv[1]
    args = sys.argv[2:]

    if command in ['-h','help'] :
        usage()

    elif command == 'add' and len(args) == 2 :
        foo = p.project_add

    elif command == 'rm' and len(args) == 1 :
        foo = p.project_remove

    elif command == 'pause' and len(args) == 1 :
        foo = p.project_pause

    elif command == 'resume' and len(args) == 1 :
        foo = p.project_resume

    elif command == 'progress' and len(args) in [0,1] :
        foo = p.project_progress

    elif command == 'when' and len(args) in [0,1] :
        foo = p.estimate_completion

    elif command == 'schedule' and len(args) == 1 :
        foo = p.scheduler_policy

    else :
        usage()


    successful,msg = foo(args)

    if not successful :
        print >> sys.stderr, "herzog: %s" % msg


if __name__ == '__main__' :
    main()

