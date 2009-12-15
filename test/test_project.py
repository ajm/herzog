from lib.projectentities import Project, Job
import sys, time

start = time.time()

p = Project('test', sys.argv[1], 'simwalk2')
p.process_background()

while True :
    j = p.next_fragment()
    if j == None :
        break

    print str(j)

end = time.time()

print "\n\ndone in %d seconds" % int(end - start)

