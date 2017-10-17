#!/usr/bin/python

from pyspark import SparkConf, SparkContext
import sys, datetime, util

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

def createLogger(logfile):
    
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    logformat = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

    # add stand out to logger
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logformat)
    logger.addHandler(ch)

    # add log file to logger
    fh = handlers.RotatingFileHandler(logfile, maxBytes=(1048576*5), backupCount=7)
    fh.setFormatter(logformat)
    logger.addHandler(fh)

    return logger

def endProcess(logf):

   if not logf.closed:
      logf.flush()
      logf.close()

def main(logf):

   util.logMessage('============ createseqfile ============', logf)
   
   vendor = sys.argv[1]
   tech = sys.argv[2]
   carr = sys.argv[3]
   inputpath = sys.argv[4]
   outputpath = sys.argv[5]
   seqFileName = outputpath.replace('.tmp', '')

   outputfd = os.path.basename(outputpath)
   outputfdarr = outputfd.split('_')
   
   dirarchive = os.path.join(inputpara['setdir'], "archive")
   if not os.path.isdir(dirarchive):
      try:
         os.mkdir(dirarchive)
      except:
         util.logMessage('unable to create archive folder: {}'.format(dirarchive), logf)
         util.logMessage(getException(), logf)
         return 1

   ####### Constants
   APP_NAME = "create sequence file - {}_{}_{}_{}_{}".format(vendor.upper(), tech.upper(), outputfdarr[3], outputfdarr[4],carr.upper())
      
   conf = SparkConf().setAppName(APP_NAME)

   sc = SparkContext(conf=conf)
   
   # source directory
   # 	hdfs  path, ex. hdfs://master.hadoop.lan:9000/user/hadoop/ericsson_lte_100
   # 	local path, ex. file:///opt/hadoop/ericsson_Manhattan
   inputpath = "file://"+sys.argv[4]

   # sequence file name to be created
   outputpath = "files://"+sys.argv[5]

   # Execute Main functionality
   try:
      contentRDD = sc.wholeTextFiles(inputpath)
   except:
      util.logMessage('spark: unable to read input folder {}'.format(inputpath), logf)
      util.logMessage(getException(), logf)
      sc.stop()
      return 1

   # save sequence 
   # 	to hdfs : hdfs://master.hadoop.lan:9000/user/hadoop/mySequenceFileName
   #	to local: file:///opt/hadoop/mySequenceFileName
   try:
      contentRDD.saveAsSequenceFile(outputpath)
   except:
      util.logMessage('spark: unable to create sequence file {}'.format(outputpath), logf)
      util.logMessage(getException(), logf)
      return 1
   else:
      sc.stop()

   util.logMessage('renameing {} to {} for parser'.format(outputpath, seqFileName), logf)
   try:
      os.rename(outputpath, seqFileName)
   except:
      util.logMessage('rename {} to {} failed'.format(outputpath, seqFileName), logf)
      util.logMessage(getException(), logf)
      return 1

   # archive input folder
   archivepath = os.path.join(dirarchive, os.path.basename(inputpath) + ".tgz")
   util.logMessage('archiving sequence input file to {}'.format(archivepath), logf)
   cmd = 'tar -zcvf {} {}'.format(archivepath, inputpath)
   ret = subprocessShellExecute(cmd)
   if ret['ret']:
      util.logMessage('input archived', logf)
   else:
      util.logMessage('failed to archive input file', logf)
      util.logMessage('error: {}'.format(ret['msg']), logf)
      return 1
            
   # remove input folder
   util.logMessage('removing sequence input file: {}'.format(inputpath), logf)
   try:
      util.removeDir(inputpath)
   except:
      util.logMessage('remove sequence input file {} failed'.format(inputpath), logf)
      util.logMessage(getException(), logf)
      return 1
   
   return 0
     
if __name__ == "__main__":

   try:
      vendor = sys.argv[1]
      tech = sys.argv[2]
      carr = sys.argv[3]
      inputpath = sys.argv[4]
      outputpath = sys.argv[5]
   except:
      util.logMessage('Incorrect parameters')
      util.logMessage(getException())
      return 1

   dirroot = os.path.dirname(os.path.realpath(__file__))
   logpath = os.path.join(dirroot, "..", "..", "log", "createseqfile")

   outputfd = os.path.basename(outputpath)
   outputfdarr = outputfd.split('_')

   if len(outputfdarr) < 6:
      util.logMessage('Incorrect output folder name: {}'.format(outputfd))
      return 1
            
   logfilename = 'createseqfile_{}_{}_{}_{}_{}.log'.format(vendor, tech, outputfdarr[3], outputfdarr[4], carr)
   logfile = os.path.join(logpath, logfilename)
   logf = None
   logfile = os.path.join(logpath, logname + ".log")
   try:
      logf = open(logfile, "w", 0)
   except IOError, e:
      util.logMessage(e.errno)
      util.logMessage(getException())
      pass

   start = datetime.datetime.now()
   ret = main(logf)
   done = datetime.datetime.now()
   
   elapsed = done - start
   util.logMessage("Process Time : {}".format(elapsed), logf)
   endProcess(logf)
   
   os._exit(ret)



