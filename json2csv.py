#!/usr/bin/python

import ujson
from sys import stdin,stdout

delim = "|"

for line in stdin:
    jsonAsDict = ujson.loads(line)
    #print jsonAsDict.values()
    stdout.write(delim.join([str(v) for v in jsonAsDict.values()]) + "\n")
    
