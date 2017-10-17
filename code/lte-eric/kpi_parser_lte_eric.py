#!/usr/bin/python

## Imports
import sys
import os
import time
import datetime
import uuid
import shutil # move file

from pyspark import SparkConf, SparkContext
from pyspark import SparkFiles

from random import random
from operator import add

import socket

from xmlparser_lte_eric import XMLParser
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
# argv[5] - extra log location

if len(sys.argv) < 4:
   util.logMessage("Error: param incorrect.")
   sys.exit(2)

# global - ssh copy
bSshCopy = False

# get proc time - [0] proc date yyyymmdd; [1] proc time hhmmssiii (last 3 millisec)
procDatetimeArr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
procDatetimeArr[1] = procDatetimeArr[1][:-3]


APP_NAME = "kpiRawApp"
# argv[1] - take app name from param
if len(sys.argv) > 1:
   APP_NAME = sys.argv[1]

# argv[2] - input file
# e.g. ttskpiraw_[vendor]_[tech]_20170614_150027076_[carr].seq
inSeqFile = sys.argv[2]

# create archive folder on the same level of input file
inputDir, inputSeq = os.path.split(inSeqFile) # .../staging/file.seq
baseDir, dirTemp = os.path.split(inputDir)
archiveDir = os.path.join(baseDir, 'archive') # .../archive/
if not os.path.isdir(archiveDir): # create if not exist
   try:	
      os.mkdir(archiveDir)
   except:
      util.logMessage("Failed to create archive folder \"%s\"!" % archiveDir)
      util.logMessage("Process terminated.")
      sys.exit(2)

# create work folder on the same level of input file
output_dir = inSeqFile + '_tmp' + procDatetimeArr[0] + procDatetimeArr[1]
if not os.path.isdir(output_dir): # create if not exist
   try:	
      os.mkdir(output_dir)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % output_dir)
      util.logMessage("Process terminated.")
      sys.exit(2)

# argv[3] - output dir
# e.g. "imnosrf@mesos_fs_01|/nfs/ttskpiraw/[tech]-[vendor]/aggregatorInput"
#      "/nfs/ttskpiraw/[tech]-[vendor]/aggregatorInput" (local copy/move)
outfilename = sys.argv[3]

# argv[4] - process mode
proc_mode = ''
if len(sys.argv) > 4:
   proc_mode = sys.argv[4]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'

# argv[5] - extra log location
extraLogLocation = ''
extraLogFilePt = None
if len(sys.argv) > 5:
   extraLogLocation = sys.argv[5]
if extraLogLocation != '':
   extraLogLocation = extraLogLocation.rstrip('/')
   extraLogFile = extraLogLocation + "/" + APP_NAME + ".log"
   try:
      extraLogFilePt = open(extraLogFile,'w')
   except:
      util.logMessage("Failed to create file \"%s\"!" % extraLogFile)
      util.logMessage("Process terminated.")
      sys.exit(2)







##OTHER FUNCTIONS/CLASSES

def f_map(filetuple):
   [fn,bw] = filetuple

   try:
      xml = XMLParser(fn,'bytes','file',bw,SparkFiles.get('config.ini'))
      #util.logMessage("inside map func, after XMLParser(), before xml.ProcessXML()")
      return xml.ProcessXML()
   except Exception as e:
      util.logMessage("err in file: %s" % fn)
      return ""

def f_reduce(a, b):
   #util.logMessage("inside reduce func")
   return a + b



def main(sc,inSeqFile,outfilename):

   global bSshCopy


   '''
   # get input file location and server addr for secure copy
   infilename = inSeqFile
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
   
   inSeqFile = internal_input_dir+'/'+infile_filename
   '''


   # get input and output file location and server addr for secure copy
   outfile_arr = outfilename.split('|')
   if len(outfile_arr) < 2:
      bSshCopy = False
   else:
      bSshCopy = True

   if bSshCopy:
      outfile_user = outfile_arr[0].split('@')[0]
      outfile_addr = outfile_arr[0].split('@')[1]
      outfile_path = outfile_arr[1]
      outfile_dir, outfile_filename = os.path.split(outfile_path)  
   else:
      outfile_path = outfilename
      

   try:

      # read ini
      configIni = XMLParser.ReadConfigIni(curr_py_dir+'/config.ini')
      global sc_configIni
      sc_configIni = sc.broadcast(configIni)

      if proc_mode == 'client':
         # add file
         #util.logMessage("addFile: %s" % curr_py_dir+'/config.ini')
         sc.addFile(curr_py_dir+'/config.ini')

         # add py reference
         #util.logMessage("addPyFile: %s" % curr_py_dir+'/xmlparser_lte_eric.py')
         sc.addPyFile(curr_py_dir+'/xmlparser_lte_eric.py')
         #util.logMessage("addPyFile: %s" % curr_py_dir+'/util.py')
         sc.addPyFile(curr_py_dir+'/util.py')

      #util.logMessage(socket.gethostname())

      # read file
      util.logMessage("reading file: %s" % inSeqFile, extraLogFilePt)
      textRDD = sc.sequenceFile(inSeqFile)
      util.logMessage("finish reading file: %s" % inSeqFile, extraLogFilePt)


   except Exception as e:

      util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
      os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e), extraLogFilePt)
      raise

   except:

      util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
      os.system("rm -rf \'%s\'" % output_dir) 
      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME, extraLogFilePt)
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

         util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e), extraLogFilePt)
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec), extraLogFilePt)
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num), extraLogFilePt)

      except socket.timeout as e:

         util.logMessage("Job: %s: Socket Timeout: %s!" % (APP_NAME, e), extraLogFilePt)
         curr_try_num += 1 # increment retry count
         if curr_try_num < socket_retry_num:
            util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec), extraLogFilePt)
            time.sleep(socket_retry_sec)
         else:
            util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num), extraLogFilePt)

      except Exception as e: # trying to catch could not open socket

         if hasattr(e, 'message') and e.message == 'could not open socket':

            util.logMessage("Job: %s: Socket Error: %s!" % (APP_NAME, e), extraLogFilePt)
            curr_try_num += 1 # increment retry count
            if curr_try_num < socket_retry_num:
               util.logMessage("Job: %s: will retry in %d sec" % (APP_NAME, socket_retry_sec), extraLogFilePt)
               time.sleep(socket_retry_sec)
            else:
               util.logMessage("Job: %s: too many retry (%d)! Give up!" % (APP_NAME, socket_retry_num), extraLogFilePt)

         else:

            util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
            os.system("rm -rf \'%s\'" % output_dir) 
            util.logMessage("Job: %s: Other Exception Error: %s!" % (APP_NAME, e), extraLogFilePt)
            raise # not the error we are looking for

      except:

         util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
         os.system("rm -rf \'%s\'" % output_dir) 
         util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME, extraLogFilePt)
         raise # not the error we are looking for

      else:

         bSocketConnFail = False




   if bSocketConnFail: # max retry reached, still fail
      util.logMessage("socket issue(s), failed parsing job: %s" % APP_NAME, extraLogFilePt)
      util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
      os.system("rm -rf \'%s\'" % output_dir) 
      return 1




   # print to file
   # e.g. ttskpiraw_[vendor]_[tech]_20170614_150027076_[carr].txt
   outputFile = inputSeq.split('.')[0] + '.txt'
   output_filename = output_dir + '/' + outputFile
   err_filename = output_filename.split('.txt')[0] + '.error.txt'
   input_gz_file = inputSeq + '.tgz'
   output_gz_file = inputSeq.split('.')[0] + '.tgz'

   util.logMessage("start writing file: %s" % output_filename, extraLogFilePt)

   err = ""
   rslt = ""
   with open(output_filename,'w') as fout:
      uuidstr = str(uuid.uuid4())
      mycache = list()
      util.logMessage("\tgetting file header - GetHeader()...", extraLogFilePt)
      header = XMLParser.GetHeader(curr_py_dir+'/config.ini')
      util.logMessage("\tgetting file header completed. Writing header...", extraLogFilePt)
      fout.write(header+'\n')
      util.logMessage("\twriting file header completed. Getting data content - GetReport()...", extraLogFilePt)
      [rslt, err] = XMLParser.GetReport(redRDD, mycache)
      util.logMessage("\tgetting data content completed. Writing data content...", extraLogFilePt)
      for row in rslt.split("\n"):
         if len(row) > 1:
            fout.write(uuidstr+','+row+'\n')
      util.logMessage("\twriting data content completed. Writing log content...", extraLogFilePt)

   # output error log
   with open(err_filename,'w') as ferr:
      ferr.write(err)
      util.logMessage("\twriting log content completed.", extraLogFilePt)

   util.logMessage("finish writing file: %s" % output_filename, extraLogFilePt)


   # stop spark context
   sc.stop()

   try:


      # zip input file into archive
      input_gz_path = archiveDir+'/'+input_gz_file
      util.logMessage('Zipping input files into archive: cd %s && tar -cvzf %s %s' % (inputDir, input_gz_path, inputSeq), extraLogFilePt)
      os.system("cd %s && tar -cvzf %s %s" % (inputDir, input_gz_path, inputSeq))
      util.logMessage('Removing seq files: %s' % inSeqFile, extraLogFilePt)
      os.system("rm -rf \'%s\'" % inSeqFile)


      if proc_mode == 'cluster':
         # copy std logs into output      
         util.logMessage('Copying logs', extraLogFilePt)
         sys.stdout.flush()
         sys.stderr.flush()
         os.system("cp std* \'%s\'" % output_dir)


      # zip folder into file
      output_gz_path = output_dir+'/'+output_gz_file
      util.logMessage('Zipping files: cd %s && tar -cvzf %s *' % (output_dir, output_gz_path), extraLogFilePt)
      os.system("cd %s && tar -cvzf %s *" % (output_dir, output_gz_path))

      if bSshCopy: # remote copy

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
         util.logMessage('Copying to remote location @ %s: %s' % (outfile_addr, outfile_path+'/'+output_gz_file+'.tmp'), extraLogFilePt)
         ret = util.copyFileToRemote2(outfile_addr, outfile_user, 'tts1234', output_gz_path, outfile_path+'/'+output_gz_file+'.tmp')
         if not ret['ret']:
            #util.logMessage('ret: %s' % ret) # cmd, ret, retCode, errmsg, outmsg
            util.logMessage('Copy to remote location failed: %s - Error Code: %s' % (outfile_path+'/'+output_gz_file+'.tmp', ret['retCode']), extraLogFilePt)
            util.logMessage('Error Msg: %s' % ret['errmsg'], extraLogFilePt)
            #sys.exit(1)
            return ret['retCode']

         util.logMessage('Finished copying to remote location @ %s: %s' % (outfile_addr, outfile_path+'/'+output_gz_file+'.tmp'), extraLogFilePt)

         # remote mv .tmp to .tgz (atomic)
         util.logMessage('Moving to remote location @ %s: %s' % (outfile_addr, outfile_path+'/'+output_gz_file), extraLogFilePt)
         ret = util.renameRemoteFile2(outfile_addr, outfile_user, 'tts1234', outfile_path+'/'+output_gz_file+'.tmp', outfile_path+'/'+output_gz_file)
         if not ret['ret']:
            #util.logMessage('ret: %s' % ret) # cmd, ret, retCode, errmsg, outmsg
            util.logMessage('Move to remote location failed: %s - Error Code: %s' % (outfile_path+'/'+output_gz_file, ret['retCode']), extraLogFilePt)
            util.logMessage('Error Msg: %s' % ret['errmsg'], extraLogFilePt)
            #sys.exit(1)
            return ret['retCode']

         util.logMessage('Finished moving to remote location @ %s: %s' % (outfile_addr, outfile_path+'/'+output_gz_file), extraLogFilePt)

      else: # local move

         # mv .tmp to .tgz (atomic)
         util.logMessage('Moving to local location: %s' % (outfile_path+'/'+output_gz_file), extraLogFilePt)
         try:
            shutil.move(output_gz_path, outfile_path+'/'+output_gz_file)            
            util.logMessage('Finished moving to local location: %s' % (outfile_path+'/'+output_gz_file), extraLogFilePt)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (output_gz_path, e), extraLogFilePt)
         except:
            util.logMessage("Unexpected error", extraLogFilePt)


   except Exception as e:

      util.logMessage("Job: %s: Exception Error: %s!" % (APP_NAME, e), extraLogFilePt)
      raise # not the error we are looking for

   except:

      util.logMessage("Job: %s: Other Unknown Error!" % APP_NAME, extraLogFilePt)
      raise # not the error we are looking for

   finally: 

      # cleanup - remove local output file
      util.logMessage('Cleanup location \'%s\'' % output_dir, extraLogFilePt)
      os.system("rm -rf \'%s\'" % output_dir) 



   return 0






if __name__ == "__main__":

   '''
   # sample
   conf = SparkConf().setAppName(APP_NAME) \
      .setMaster("spark://master:7077") \
      .set("spark.executor.memory", "1g") \
      .set("spark.driver.memory", "1g") \
      .set("spark.executor.cores", "1") \
      .set("spark.cores.max", num_core)
   '''

   try:
      # Configure Spark
      conf = SparkConf().setAppName(APP_NAME)
      #conf = conf.setMaster("mesos://mesos_master_01:7077")
      sc = SparkContext(conf=conf)

      # Execute Main functionality
      ret = main(sc, inSeqFile, outfilename)
      if not ret == 0: 
         sys.exit(ret)
   except SystemExit as e:
      if e.code == 0: # no problem
         pass
      else: # other exception
         raise
   except Exception as e:
      util.logMessage("Error: Parser Proc exception occur\n%s" % e, extraLogFilePt)
      util.logMessage("Process terminated.", extraLogFilePt)
      sys.exit(2)
   except:
      util.logMessage("Unexpected error", extraLogFilePt)
      util.logMessage("Process terminated.", extraLogFilePt)
      sys.exit(2)



