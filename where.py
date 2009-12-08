#!/usr/bin/env python
import xmlrpclib

p = xmlrpclib.ServerProxy('http://gatsby:8001')
tmp = p.list_current()

s = "%s\t%s:%s"
#print s % ("fragment","hostname","remote dir")
#print "-" * 100
for k,v in tmp.items() :
    localpath,hostname = v
    print s % (localpath,hostname,k)

#    print v,
#    print "\t",
#    print k

