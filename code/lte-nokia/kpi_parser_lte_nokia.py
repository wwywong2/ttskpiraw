#!/usr/bin/python

## Imports
import sys
import os
import time
import uuid
import copy

from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles

from random import random
from operator import add

import socket

from xmlparser_lte_nokia import nokiaXmlParser
import util


## Constants
socket_retry_sec = 5 # num of sec to wait for next retry when socket fail
socket_retry_num = 10 # num of times to retry when socket fail



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path
#curr_py_dir = os.path.dirname(curr_py_path)

# argv[1] - process name
# argv[2] - input file
# argv[3] - output dir
# argv[4] - process mode: 'client' or 'cluster'

APP_NAME = "Read seq xml file (w/ map reduce)"
# argv[1] - take app name from param
if len(sys.argv) > 1:
   APP_NAME = sys.argv[1]


# argv[3] - output dir
output_dir = ""
#if len(sys.argv) > 3:
#   output_dir = sys.argv[3]
#output_dir = output_dir.rstrip('/')
output_dir = curr_py_dir+'/output_'+time.strftime("%Y%m%d%H%M%S")

if output_dir == "":
   output_dir = "." # default current folder
elif not os.path.isdir(output_dir): # create if not exist
   try:	
      os.mkdir(output_dir)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % output_dir)
      util.logMessage("Process terminated.")
      sys.exit(2)
else:
   pass



# argv[4] - process mode
proc_mode = ''
if len(sys.argv) > 4:
   proc_mode = sys.argv[4]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'





##OTHER FUNCTIONS/CLASSES

def f_map(filetuple):
   [fn,bw] = filetuple

   try:
      xml = nokiaXmlParser(bw, SparkFiles.get('config.ini'), fn)
      #util.logMessage("inside map func, after nokiaXmlParser(), before xml.Main()")
      data, type, err =  xml.Main()
      #print fn, type, data

      obj = dict()
      obj['err'] = err
      obj['data'] = data
      xmlDict = { type: obj } # list of detail [0] and nb summary [1] (if exists)
      return xmlDict

   except Exception as e:
      util.logMessage("err in file: %s\n%s" % (fn, e))
      return {}

def f_reduce(a, b):
   #util.logMessage("inside reduce func")

   b_new = {}
   for key,value in b.iteritems():
      if key in a:
         value['err'][0] = a[key]['err'][0] + value['err'][0] # normal error
         value['err'][1] = a[key]['err'][1] + value['err'][1] # nothing use error
         value['data'][0] = a[key]['data'][0] + value['data'][0]
         value['data'][1] = a[key]['data'][1] + value['data'][1]
         b_new[key] = value
      else:
         b_new[key] = value

   a_new = copy.deepcopy(a)
   a_new.update(b_new)

   return a_new



def main(sc,filename,outfilename):



   '''
   # get input file location and server addr for secure copy
   infilename = filename
   infile_arr = infilename.split('|')
   if len(infile_arr) < 2:
      util.logMessage("Error: file not found: %s" % infilename)
   infile_user = infile_arr[0].split('@')[0]
   infile_addr = infile_arr[0].split('@')[1]
   infile_path = infile_arr[1]
   infile_dir, infile_filename = os.path.split(infile_path)  

   
   # check remote file existence  
   ret = util.checkRemoteFileExist(infile_addr, infile_user, 'tts1234', infile_path)
   if not ret['ret']:
      util.logMessage('Remote input file does not exist: %s' % infile_path)
      sys.exit(1)

   # copy file to internal location
   util.logMessage('Copying remote input file @ %s: %s' % (infile_addr, infile_path))

   internal_input_dir = curr_py_dir+'/../input'
   ret = util.copyRemoteFile(infile_addr, infile_user, 'tts1234', infile_path, internal_input_dir+'/')
   if not ret['ret']:
      util.logMessage('Copy remote input file failed: %s' % infile_path)
      sys.exit(1)

   util.logMessage('Finished copying remote input file to local: %s' % internal_input_dir+'/'+infile_filename)
   
   filename = internal_input_dir+'/'+infile_filename
   '''


   # get input and output file location and server addr for secure copy
   outfile_arr = outfilename.split('|')
   if len(outfile_arr) < 2:
      util.logMessage("Error: output file wrong format: %s" % outfilename)
   outfile_user = outfile_arr[0].split('@')[0]
   outfile_addr = outfile_arr[0].split('@')[1]
   outfile_path = outfile_arr[1]
   outfile_dir, outfile_filename = os.path.split(outfile_path)  
   

   try:
      # read ini
      #configIni = XMLParser.ReadConfigIni(curr_py_dir+'/config.ini')
      #global sc_configIni
      #sc_configIni = sc.broadcast(configIni)

      if proc_mode == 'client':
         # add file
         #util.logMessage("addFile: %s" % curr_py_dir+'/config.ini')
         sc.addFile(curr_py_dir+'/config.ini')

         # add py reference
         #util.logMessage("addPyFile: %s" % curr_py_dir+'/xmlparser_lte_nokia.py')
         sc.addPyFile(curr_py_dir+'/xmlparser_lte_nokia.py')
         #util.logMessage("addPyFile: %s" % curr_py_dir+'/util.py')
         sc.addPyFile(curr_py_dir+'/util.py')

      #util.logMessage(socket.gethostname())

      # read file
      util.logMessage("reading file: %s" % filename)
      textRDD = sc.sequenceFile(filename)
      #textRDD = sc.wholeTextFiles(filename)
      util.logMessage("finish reading file: %s" % filename)


   except Exception as e:

      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise

   except:

      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for



   #print textRDD.collect()

   curr_try_num = 0
   bSocketConnFail = True # init to True so it will go into loop
   while (curr_try_num < socket_retry_num and bSocketConnFail):
     
      try: 

         # map
         #util.logMessage("starting map")
         mapRDD = textRDD.map(f_map)
         #util.logMessage("after map, starting reduce")

         #print mapRDD.count()
         #print mapRDD.collect()


         # reduce
         redRDD = mapRDD.reduce(f_reduce)
         #util.logMessage("after reduce")

         #print redRDD.count()
         #print redRDD.collect()


      except socket.error as e:

         util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e))
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

      except socket.timeout as e:

         util.logMessage("Job: %s: Socket Timeout: %s!" % (APP_NAME, e))
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

      except Exception as e: # trying to catch could not open socket

         if hasattr(e, 'message') and e.message == 'could not open socket':

            util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e))
            curr_try_num += 1 # increment retry count
            if curr_try_num < socket_retry_num:
               util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec))
               time.sleep(socket_retry_sec)
            else:
               util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num))

         else:

            util.logMessage('Cleanup location \'%s\'' % output_dir)
            os.system("rm -rf \'%s\'" % output_dir) 
            util.logMessage("Job: %s: Other Exception Error: %s!" % (APP_NAME, e))
            raise # not the error we are looking for

      except:

         util.logMessage('Cleanup location \'%s\'' % output_dir)
         os.system("rm -rf \'%s\'" % output_dir) 
         util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
         raise # not the error we are looking for

      else:

         bSocketConnFail = False




   if bSocketConnFail: # max retry reached, still fail
      util.logMessage("socket issue(s), failed parsing job: %s" % APP_NAME)
      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      return 1




   # print to file
   input_dir, input_filename = os.path.split(filename + '.txt')  # current file and folder - abs path
   input_dir, output_gz_file = os.path.split(filename + '_' + time.strftime("%Y%m%d%H%M%S") + '.tgz')
   output_filename = output_dir + '/' + input_filename

   util.logMessage("start writing file: %s" % output_filename.split('.txt')[0] + "_*.txt")

   uuidstr = "NA"
   if len(redRDD) <= 0:
      util.logMessage("No data to write to file: %s" % output_filename.split('.txt')[0] + "_*.txt")
   else:
      uuidstr = str(uuid.uuid4())

   for key, value in redRDD.iteritems():
      header, err = nokiaXmlParser.GetHeaderIni(curr_py_dir+'/config.ini', key)
      if (len(err) == 0):

         # print error (if exist)
         if len(value['err']) > 0: # have error

            if len(value['err']) > 0: # regular error
               newfilename = output_filename.split('.txt')[0] + "_" + key + ".error.txt"
               if len(value['err'][0]) > 0:
                  with open(newfilename,'w') as ferr:
                     for row in value['err'][0]:
                        if row != "":
                           ferr.write(row+'\n')

            if len(value['err']) > 1: # nothing use error
               newfilename2 = output_filename.split('.txt')[0] + "_" + key + ".errorNothingUse.txt"
               if len(value['err'][1]) > 0:
                  with open(newfilename2,'w') as ferr2:
                     for row in value['err'][1]:
                        if row != "":
                           ferr2.write(row+'\n')

         # print detail         
         newfilename = output_filename.split('.txt')[0] + "_" + key + ".txt"
         if len(value['data'][0]) > 0: # have detail lines
            with open(newfilename,'w') as fout:
               fout.write(header+'\n')
               for row in value['data'][0]:
                  if row != "":
                     fout.write(uuidstr+'|'+row+'\n')
         #else:
         #   print "[%s] No data to write to file: %s" % (
         #      time.strftime("%Y-%m-%d %H:%M:%S"), newfilename)
         #   sys.stdout.flush()


         # print summary (if exist)
         if len(value['data'][1]) > 0: # have summary lines
            # get summary header
            header2, err2 = nokiaXmlParser.GetHeaderIniNB(curr_py_dir+'/config.ini', key)
            newfilename = output_filename.split('.txt')[0] + "_" + key + "_sum.txt"
            if (len(err2) == 0):
               with open(newfilename,'w') as foutsum:
                  foutsum.write(header2+'\n')
                  for row in value['data'][1]:
                     if row != "":
                        foutsum.write(uuidstr+'|'+row+'\n')

   util.logMessage("finish writing file: %s" % output_filename.split('.txt')[0] + "_*.txt")


   # stop spark context
   sc.stop()

   try:

      if proc_mode == 'cluster':
         # copy std logs into output      
         util.logMessage('Copying logs')
         sys.stdout.flush()
         sys.stderr.flush()
         os.system("cp std* \'%s\'" % output_dir)


      # zip folder into file
      output_gz_path = curr_py_dir+'/'+output_gz_file
      util.logMessage('Zipping files: cd %s && tar -cvzf %s *' % (output_dir, output_gz_path))
      os.system("cd %s && tar -cvzf %s *" % (output_dir, output_gz_path))


      # copy file to external location
      # method 1 (cannot take '*'): recursive: .../output --> .../result/output/*
      '''
      util.logMessage('Copying to remote location @ %s: %s' % (outfile_addr, outfile_path))
      ret = util.copyFileToRemote1(outfile_addr, outfile_user, 'tts1234', output_dir, outfile_path)
      if not ret['ret']:
         #util.logMessage('ret: %s' % ret) # cmd, ret, retCode
         util.logMessage('Copy to remote location failed: %s - Error Code: %s' % (outfile_path, ret['retCode']))
         sys.exit(1)
      '''

      # method 2 (take '*'): recursive: .../output/* --> .../result/*
      util.logMessage('Copying to remote location @ %s: %s' % (outfile_addr, outfile_path))
      #ret = util.copyFileToRemote2(outfile_addr, outfile_user, 'tts1234', output_dir+'/*', outfile_path)
      ret = util.copyFileToRemote2(outfile_addr, outfile_user, 'tts1234', output_gz_path, outfile_path)
      if not ret['ret']:
         #util.logMessage('ret: %s' % ret) # cmd, ret, retCode, errmsg, outmsg
         util.logMessage('Copy to remote location failed: %s - Error Code: %s' % (outfile_path, ret['retCode']))
         util.logMessage('Error Msg: %s' % ret['errmsg'])
         #sys.exit(1)
         return ret['retCode']

      util.logMessage('Finished copying to remote location @ %s: %s' % (outfile_addr, outfile_path))

   
   except Exception as e:

      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e))
      raise # not the error we are looking for

   except:

      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME)
      raise # not the error we are looking for

   finally: 

      # cleanup - remove local output file
      util.logMessage('Cleanup location \'%s\'' % output_dir)
      os.system("rm -rf \'%s\'" % output_dir) 
      os.system("rm -f \'%s\'" % output_gz_path) 



   return 0






if __name__ == "__main__":

   if len(sys.argv) < 4:
      util.logMessage("Error: param incorrect.")
      sys.exit(2)

   filename = sys.argv[2]
   outfilename = sys.argv[3]

   '''
   # sample
   conf = SparkConf().setAppName(APP_NAME) \
      .setMaster("spark://master:7077") \
      .set("spark.executor.memory", "1g") \
      .set("spark.driver.memory", "1g") \
      .set("spark.executor.cores", "1") \
      .set("spark.cores.max", num_core)
   '''
   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   #conf = conf.setMaster("spark://master:7077")
   sc = SparkContext(conf=conf)

   # Execute Main functionality
   ret = main(sc, filename, outfilename)
   if not ret == 0: 
      sys.exit(ret)
