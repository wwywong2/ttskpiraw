#!/usr/bin/python

from pyspark import SparkConf, SparkContext
import os, sys, datetime, util, linecache, subprocess
import traceback, logging
from logging.handlers import RotatingFileHandler
from logging import handlers

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
    logger.setLevel(logging.INFO)
    logformat = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

    # add stand out to logger
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logformat)
    logger.addHandler(ch)

    # add log file to logger
    fh = handlers.RotatingFileHandler(logfile, maxBytes=(1048576*5), backupCount=1)
    fh.setFormatter(logformat)
    logger.addHandler(fh)

    return logger

################################################
#   subprocessShellExecute
#       1 . MySQL
#       2.  Execuable
################################################
def subprocessShellExecute(cmd):
    retObj = {}
    p = subprocess.Popen(cmd, shell=True, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        # an error happened!
        err_msg = "%s. Code: %s" % (err.strip(), p.returncode)
        retObj['ret'] = False
        retObj['msg'] = err_msg  
    else:
        retObj['ret'] = True
        if len(err): # warning
            retObj['msg'] = err
        retObj['output'] = out
    #p.kill()
    return retObj

def endProcess(dirarchive, inputpath, start, logf):

    inputdir = os.path.dirname(inputpath)
    inputfd = os.path.basename(inputpath)

    # archive input folder
    archivepath = os.path.join(dirarchive, inputfd.replace(".lock", ""))
    archivegzpath = os.path.join(dirarchive, inputfd.replace(".lock", "") + ".tgz")
    try:
        util.logMessage('moving input {} to {} ...'.format(inputpath, archivepath), logf)
        os.rename(inputpath, archivepath)
    except:
        util.logMessage('move input {} to {} failed'.format(inputpath, archivepath), logf)
        util.logMessage(getException(), logf)
    else:
        util.logMessage('archiving sequence input file to {}'.format(archivegzpath), logf)
        cmd = 'tar -C {} -zcvf {} {}'.format(dirarchive, archivegzpath, inputfd.replace(".lock", ""))
        util.logMessage('archive cmd: {}'.format(cmd), logf)
        ret = subprocessShellExecute(cmd)
        if ret['ret']:
            util.logMessage('input archived', logf)
        else:
            util.logMessage('failed to archive input file', logf)
            util.logMessage('error: {}'.format(ret['msg']), logf)

        # remove input folder
        util.logMessage('removing sequence input file: {}'.format(archivepath), logf)
        try:
            util.removeDir(archivepath)
        except:
            util.logMessage('remove sequence input file {} failed'.format(archivepath), logf)
            util.logMessage(getException(), logf)
            pass
    finally:
        done = datetime.datetime.now()
        elapsed = done - start
        util.logMessage("Process Time : {}".format(elapsed), logf)
        
        if not logf.closed:
            logf.flush()
            logf.close()

def main():

    vendor = sys.argv[1]
    tech = sys.argv[2]
    carr = sys.argv[3]
    inputpath = sys.argv[4]
    outputpath = sys.argv[5]
    seqFileName = outputpath.replace('.tmp', '')

    dirroot = os.path.dirname(os.path.realpath(__file__))
    logpath = os.path.join(dirroot, "..", "..", "log", "createseqfile")

    start = datetime.datetime.now()
    datetimearr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')

    inputfd = os.path.basename(inputpath)
    inputfdarr = inputfd.split('_')
    if len(inputfdarr) < 7:
        util.logMessage('Incorrect input folder name: {}'.format(inputfd), logf)
        return 1
    
    # log file
    logf = None
    logfile = os.path.join(logpath, 'createseqfile_{}_{}_{}_{}_{}_{}_{}.log'\
                .format(inputfdarr[0], inputfdarr[1], inputfdarr[2], inputfdarr[3], inputfdarr[4], inputfdarr[5], inputfdarr[6]))
    try:
        logf = open(logfile, "w", 0) 
    except IOError, e:
        util.logMessage(e.errno)
        util.logMessage(getException())
        pass

    util.logMessage('============ create sequence file ============', logf)
    util.logMessage('input parameters: vendor: {}, tech: {}, carrier: {}'.format(vendor, tech, carr), logf)
    util.logMessage('input parameters: input path: {}, output path: {}'.format(inputpath, outputpath), logf)
    
    seqoutputtmpfd = "ttskpiraw_{}_{}_{}_{}_{}.seq.tmp".format(vendor.upper(), tech.upper() \
                                                         , datetimearr[0], datetimearr[1][:-3], carr.upper())
    seqoutputfd = "ttskpiraw_{}_{}_{}_{}_{}.seq".format(vendor.upper(), tech.upper() \
                                                         , datetimearr[0], datetimearr[1][:-3], carr.upper())
    seqoutputtmppath = os.path.join(outputpath, seqoutputtmpfd)
    seqoutputpath = os.path.join(outputpath, seqoutputfd)

    util.logMessage('temporarily sequence file output path: {}'.format(seqoutputtmppath), logf)
    util.logMessage('final sequence file output path: {}'.format(seqoutputpath), logf)

    inputdir = os.path.dirname(inputpath)
    dirarchive = os.path.join(inputdir, "archive")
    if not os.path.isdir(dirarchive):
        util.logMessage('archive path does not exist: {}'.format(dirarchive), logf)
        try:
            util.logMessage('creating archive path: {}'.format(dirarchive), logf)
            os.mkdir(dirarchive)
        except:
            util.logMessage('unable to create archive folder: {}'.format(dirarchive), logf)
            util.logMessage(getException(), logf)
            pass

    errdirarchive = os.path.join(inputdir, "archive", "failed_set")
    if not os.path.isdir(errdirarchive):
        util.logMessage('archive path for storing failed set does not exist: {}'.format(errdirarchive), logf)
        try:
            util.logMessage('creating archive path for storing failed set: {}'.format(errdirarchive), logf)
            os.mkdir(errdirarchive)
        except:
            util.logMessage('unable to create archive folder for failed set: {}'.format(errdirarchive), logf)
            util.logMessage(getException(), logf)
            pass

    ####### Constants
    APP_NAME = "create sequence file - {}_{}_{}_{}_{}".format(vendor.upper(), tech.upper(), datetimearr[0], datetimearr[1][:-3], carr.upper())
    util.logMessage('spark app name: {}'.format(APP_NAME))
    
    conf = SparkConf().setAppName(APP_NAME)
    sc = SparkContext(conf=conf)
            
    socket_retry_sec = 5 # num of sec to wait for next retry when socket fail
    socket_retry_num = 10 # num of times to retry when socket fail
    curr_try_num = 0
    bSocketConnFail = True # init to True so it will go into loop
    while (curr_try_num < socket_retry_num and bSocketConnFail):

        try:

            # source directory
            # 	hdfs  path, ex. hdfs://master.hadoop.lan:9000/user/hadoop/ericsson_lte_100
            # 	local path, ex. file:///opt/hadoop/ericsson_Manhattan
            sparkinputpath = "file://{}".format(inputpath)
            util.logMessage('spark job input path: {}'.format(sparkinputpath), logf)
    
            # sequence file name to be created
            sparkoutputpath = "file://{}".format(seqoutputtmppath)
            util.logMessage('spark job output path: {}'.format(sparkoutputpath), logf)

            # Execute Main functionality
            util.logMessage('reading whole text files from: {}'.format(sparkinputpath), logf)
            contentRDD = sc.wholeTextFiles(sparkinputpath)
            util.logMessage('finish reading whole text files', logf)
            
            # save sequence
            # 	to hdfs : hdfs://master.hadoop.lan:9000/user/hadoop/mySequenceFileName
            #	to local: file:///opt/hadoop/mySequenceFileName
            util.logMessage('saving sequence file to: {}'.format(sparkoutputpath), logf)
            contentRDD.saveAsSequenceFile(sparkoutputpath)
            util.logMessage('finish saving sequence file', logf)
            
        except socket.error as e:

            util.logMessage("Job: {}: Socket Error: {}!".format(APP_NAME, e), logf)
            curr_try_num += 1 # increment retry count
            if curr_try_num < socket_retry_num:
                util.logMessage("Job: {}: will retry in {} sec".format(APP_NAME, socket_retry_sec), logf)
                time.sleep(socket_retry_sec)
            else:
                util.logMessage("Job: {}: too many retry ({})! Give up!".format(APP_NAME, socket_retry_num), logf)

        except socket.timeout as e:

            util.logMessage("Job: {}: Socket Timeout: {}!".format(APP_NAME, e), logf)
            curr_try_num += 1 # increment retry count
            if curr_try_num < socket_retry_num:
                util.logMessage("Job: {}: will retry in {} sec".format(APP_NAME, socket_retry_sec), logf)
                time.sleep(socket_retry_sec)
            else:
                util.logMessage("Job: {}: too many retry ({})! Give up!".format(APP_NAME, socket_retry_num), logf)

        except Exception as e: # trying to catch could not open socket

            if hasattr(e, 'message') and e.message == 'could not open socket':

                util.logMessage("Job: {}: Socket Error: {}!".format(APP_NAME, e), logf)
                curr_try_num += 1 # increment retry count
                if curr_try_num < socket_retry_num:
                    util.logMessage("Job: {}: will retry in {} sec".format(APP_NAME, socket_retry_sec), logf)
                    time.sleep(socket_retry_sec)
                else:
                    util.logMessage("Job: {}: too many retry ({})! Give up!".format(APP_NAME, socket_retry_num), logf)

            else:
                util.logMessage("Job: {}: Other Exception Error: {}!".format(APP_NAME, getException()))
                break

        except:
            util.logMessage("Job: {}: Other Unknown Error - {}!".format(APP_NAME, getException()))
            break

        else:
            bSocketConnFail = False



    # socket failed, unlock input for next scheduler
    if bSocketConnFail: # max retry reached, still fail
        util.logMessage("socket issue(s), failed parsing job: %s" % APP_NAME)
        util.logMessage('remove temporarily sequence file output path: {}'.format(seqoutputtmppath), logf)
        try:
            util.removeDir(seqoutputtmppath)
        except:
            util.logMessage('unable to remove temporarily sequence file output path: {}'.format(seqoutputtmppath), logf)
            util.logMessage(getException(), logf)
            pass
        util.logMessage('remove final sequence file output path: {}'.format(seqoutputpath), logf)
        try:
            util.removeDir(seqoutputpath)
        except:
            util.logMessage('unable to remove final sequence file output path: {}'.format(seqoutputpath), logf)
            util.logMessage(getException(), logf)
            pass

        util.logMessage('remove input lock: {} to {}'.format(inputpath, inputpath.replace('.lock', '')))
        try:
            os.rename(inputpath, inputpath.replace('.lock', ''))
        except:
            util.logMessage('unable to remove input lock', logf)
            util.logMessage(getException(), logf)
            pass


        done = datetime.datetime.now()
        elapsed = done - start
        util.logMessage("Process Time : {}".format(elapsed), logf)
        
        if not logf.closed:
            logf.flush()
            logf.close()
            
        return 1

    # check sequence file creation
    if not os.path.isfile(os.path.join(seqoutputtmppath, "_SUCCESS")):
        util.logMessage('sequence file creation is INCOMPLETED: {}'.format(seqoutputtmppath), logf)
        endProcess(dirarchive, inputpath, start, logf)
        return 1

    util.logMessage('renameing {} to {} for parser'.format(seqoutputtmppath, seqoutputpath), logf)
    try:
        os.rename(seqoutputtmppath, seqoutputpath)
    except:
        util.logMessage('rename {} to {} failed'.format(seqoutputtmppath, seqoutputpath), logf)
        util.logMessage(getException(), logf)
        endProcess(dirarchive, inputpath, start, logf)
        return 1
    else:
        endProcess(dirarchive, inputpath, start, logf)
    
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
        os._exit(1)

    ret = main()
    os._exit(ret)



