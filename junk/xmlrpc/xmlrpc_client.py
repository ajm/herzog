#!/usr/bin/env python
import xmlrpclib

proxy = xmlrpclib.ServerProxy('http://localhost:8000')
print proxy.foo("hello")
print proxy.foo("goodbye")

