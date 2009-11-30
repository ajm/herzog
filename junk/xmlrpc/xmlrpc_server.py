#!/usr/bin/env python
import sys
import xmlrpclib
from SimpleXMLRPCServer import SimpleXMLRPCServer

def foo(n):
	return "i received %s" % n

server = SimpleXMLRPCServer(('localhost',8000))
server.register_function(foo, 'foo')

try :
	server.serve_forever()
except KeyboardInterrupt :
	pass


