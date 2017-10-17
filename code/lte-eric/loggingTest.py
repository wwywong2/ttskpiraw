#!/usr/bin/python
'''
import os
import time
import datetime
import uuid
import json
import glob
import shutil # move file
import requests
import util
'''

import time
import socket
import linecache
import logging
from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler
import sys


hostname = socket.gethostname()



def getException():
    expobj = {}

    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)

    expobj['filename'] = filename
    expobj['linenumber'] = lineno
    expobj['line'] = line.strip()
    expobj['err'] = exc_obj

    return expobj




# note: default logging if not set is directing to stderr, not stdout!
# note: default log level is only showing anything value >= warning (not showing debug and info)

# this can only call once on the top; other call no use
fmtStr = '[%%(asctime)s.%%(msecs)03d @%s] %%(name)s %%(levelname)s: %%(message)s' % hostname
logging.basicConfig(format=fmtStr, datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG, stream=sys.stdout)


# root level warning
logging.basicConfig(level=logging.DEBUG) 
logging.debug('toplevel debug %d' % logging.DEBUG) # logging.DEBUG: 10 - debug
logging.info('toplevel info %d' % logging.INFO) # logging.INFO: 20 - info
logging.warning('toplevel warning %d' % logging.WARNING) # logging.WARNING: 30 - some problem but still working
logging.error('toplevel error %d' % logging.ERROR) # logging.ERROR: 40 - more serious problem, but still running
logging.error('toplevel critical %d' % logging.CRITICAL) # logging.CRITICAL: 50 - program cannot continue running



# logging with data
logging.info('this var %d here', 34) 



# to get level value from input string
inputLevel = 'INFO'
levelVal = getattr(logging, inputLevel, None)
if levelVal is not None:
   logging.info('levelVal of "%s" is %d' % (inputLevel, levelVal))
else:
   logging.error('No such level value for "%s"!', inputLevel)



# direct logging to stdout instead of stderr (default)
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)



# logging to file (append)
'''
logging.basicConfig(filename='example.log', level=logging.DEBUG)
logging.debug('logging to file')
'''
# logging to file (overwrite)
'''
logging.basicConfig(filename='example.log', filemode='w', level=logging.DEBUG)
logging.debug('logging to file')
'''



# format
# %(levelname)s - log lvl
# %(message)s - actual msg
# %(asctime)s - date time: 2010-12-12 11:41:42,612
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p') # 07/21/2017 07:09:19 PM
# other formats
fmtStr = '[%%(asctime)s.%%(msecs)03d @%s] %%(levelname)s: %%(message)s' % hostname
fmtStr += '\n%(created)f' # 1500691860.660504
fmtStr += '|%(filename)s' # loggingTest.py
fmtStr += '|%(funcName)s' # <module>
fmtStr += '|%(levelno)s' # 20
fmtStr += '|line:%(lineno)d' # 48
fmtStr += '|%(module)s' # loggingTest
fmtStr += '|%(name)s' # root
fmtStr += '|%(pathname)s' # ./loggingTest.py (the name when this py is run (including full path)
fmtStr += '|%(process)d' # 23564
fmtStr += '|%(processName)s' # MainProcess
fmtStr += '|%(relativeCreated)d' # 1 (how many milliseconds passed from the module was loaded)
fmtStr += '|%(thread)d' # 140358583957312
fmtStr += '|%(threadName)s' # MainThread
logging.basicConfig(format=fmtStr, datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG, stream=sys.stdout)



# a good format "[time @machine] level: message"
fmtStr = '[%%(asctime)s.%%(msecs)03d @%s] %%(levelname)s: %%(message)s' % hostname
logging.basicConfig(format=fmtStr, datefmt='%Y-%m-%d %H:%M:%S', level=logging.DEBUG, stream=sys.stdout)



# module level logging
#print __name__ # '__main__'
lgr_module = logging.getLogger(__name__)
lgr_module.info('new module logger created')

lgr_module.info('test before setting ERROR')
lgr_module.setLevel(logging.ERROR)
lgr_module.info('test after setting ERROR')
lgr_module.setLevel(logging.INFO) # these will not show
lgr_module.info('test after setting INFO')

# remove existing handlers - after that by default it have stderr as handler (on first logging func calling)
for hdlr in logging.root.handlers[:]:
   logging.root.removeHandler(hdlr)



# new formatter
fmtStr = '[%%(asctime)s.%%(msecs)03d @%s] %%(name)s %%(levelname)s: %%(message)s' % hostname
formatter = logging.Formatter(fmtStr, datefmt='%Y-%m-%d %H:%M:%S')
fmtStr2 = '[%%(asctime)s.%%(msecs)03d @%s] %%(funcName)s %%(levelname)s: %%(message)s' % hostname
formatter2 = logging.Formatter(fmtStr2, datefmt='%m/%d/%Y %H:%M:%S')

# new handler
hdlr_stderr = logging.StreamHandler() # no param = stderr
hdlr_stderr.setLevel(logging.DEBUG)
hdlr_stderr.setFormatter(formatter) # setup handler with formatter

# setup logger with new handler (stderr)
lgr_module.addHandler(hdlr_stderr)
lgr_module.info('test after new handler - to stderr')

# change to new formatter
hdlr_stderr.setFormatter(formatter2)
lgr_module.info('test after new formatter - to stderr')

# remove existing handlers
lgr_module.removeHandler(hdlr_stderr)

# add new handler - stdout
hdlr_stdout = logging.StreamHandler(sys.stdout)
hdlr_stdout.setLevel(logging.DEBUG)
hdlr_stdout.setFormatter(formatter2)
lgr_module.addHandler(hdlr_stdout)
lgr_module.info('test after another new handler - to stdout')



# output to multiple places: stdout and file
hdlr_file = logging.FileHandler('example.log', mode='w')
hdlr_file.setFormatter(formatter2)
lgr_module.addHandler(hdlr_file)
lgr_module.info('test after adding file handler - to stdout and file')
lgr_module.removeHandler(hdlr_file)



# current list of handlers
lgr_module.info('# of root logger handler: %d', len(logging.root.handlers[:])) # 0 - no root logger until first call
lgr_module.info('# of module logger handler: %d', len(lgr_module.handlers[:])) # 1 - stdout (stderr removed)



# rotating log
hdlr_rfile = RotatingFileHandler('example_rotate.log', maxBytes=5, backupCount=2) # it might exceed 5 bytes but if it is bigger, the next write will be on the new file
hdlr_rfile.setFormatter(formatter2)
lgr_module.addHandler(hdlr_rfile)
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and rotate file')
lgr_module.removeHandler(hdlr_rfile) # must call or time is not correct



# timed rotating log - S M H D 'W0'-'W6'(mon-sun) 'midnight' 
hdlr_trfile = TimedRotatingFileHandler('example_timed_rotate.log', when='S', interval=1, backupCount=2) # it might exceed 5 bytes but if it is bigger, the next write will be on the new file
hdlr_trfile.setFormatter(formatter)
lgr_module.addHandler(hdlr_trfile)
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and timed rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and timed rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and timed rotate file')
time.sleep(1)
lgr_module.info('test after adding file handler - to stdout and timed rotate file')
lgr_module.removeHandler(hdlr_trfile) # must call or time is not correct



# try logger exception()
try:
   a = b + 123
except Exception as e:

   #print e # will show "name 'b' is not defined"

   # own func, but .exception() already have it all
   #eObj = getException()
   #print eObj

   lgr_module.exception('test exception')
   # ...
   #    in here it will show the usual detail error with Traceback
   #
   lgr_module.info('line after excecption()')


logging.shutdown() # need?


