#!/usr/bin/env python
import xmlrpclib

url = 'http://gatsby:8001'
#url = 'http://euclid.kleta-lab:8001'

p = xmlrpclib.ServerProxy(url)
tmp = p.list_current()

s = "%s\t%s:%s"
for k,v in tmp.items() :
    localpath,hostname = v
    print s % (localpath,hostname,k)

