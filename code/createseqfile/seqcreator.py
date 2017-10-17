#!/usr/bin/python
import os, shutil, sys, struct, datetime
import json, util, uuid, linecache, glob
import subprocess, gzip, time, logging
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
    logger.setLevel(logging.DEBUG)
    logformat = logging.Formatter("[%(asctime)s] ** %(levelname)s **: %(message)s")

    # add stand out to logger
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logformat)
    logger.addHandler(ch)

    # add log file to logger
    fh = handlers.RotatingFileHandler(logfile, maxBytes=(1048576*5), backupCount=7)
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
        if len(err): # warnning
            retObj['msg'] = err
        retObj['output'] = out
    #p.kill()
    return retObj

def main(mylog):

    '''
    python seqcreator.py ERICSSON LTE TMO /nfskpi/wyang/ttskpiraw/lte-eric/input /nfskpi/wyang/ttskpiraw/lte-eric/input_seq
    '''
    
    createseqfilepy = os.path.join(dirroot, "createseqfile.py")

    if not os.path.isfile(createseqfilepy):
        mylog.error('createseqfile.py does not exit: {}'.format(createseqfilepy))
        return 1
    mylog.info('createseqfile.py exits: {}'.format(createseqfilepy))
    
    inputpara = {}
    try:
        inputpara['vendor'] = sys.argv[1]
        inputpara['tech'] = sys.argv[2]
        inputpara['carr'] = sys.argv[3]
        inputpara['setdir'] = sys.argv[4]
        inputpara['seqdir'] = sys.argv[5]
    except:
        mylog.error('Incorrect parameters')
        mylog.debug(getException())
        return 1

    uid = str(uuid.uuid1())
    setfdpattern = 'ttskpiraw_{}_{}_*_*_{}_*.seqinput'.format(inputpara['vendor'].upper(), inputpara['tech'].upper()\
                                     , inputpara['carr'].upper())
    
    mylog.info('sequence input folder pattern: {}'.format(setfdpattern))
    
    bhasset = False
    for setfdpath in glob.glob(os.path.join(inputpara['setdir'], setfdpattern)):
        bhasset = True

        mylog.info('set folder found: {}'.format(setfdpath))
        
        seqinputfdarr = os.path.basename(setfdpath).split('_')
        if len(seqinputfdarr) < 6:
            mylog.info('set folder name incorrect: {}'.format(os.path.basename(setfdpath)))
            continue

        setfdpathlock = setfdpath + ".lock"
        mylog.info('rename (lock) folder from {} to {}'.format(setfdpath, setfdpathlock))
        try:
            os.rename(setfdpath, setfdpathlock)
        except:
            mylog.error('unable to renema (lock) folder: {}'.format(setfdpath))
            mylog.debug(getException())
            continue

        datetimearr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
        seqoutputtmpfd = "ttskpiraw_{}_{}_{}_{}_{}.seq.tmp".format(inputpara['vendor'].upper(), inputpara['tech'].upper() \
                                                         , datetimearr[0], datetimearr[1], inputpara['carr'].upper())
        seqoutputfd = "ttskpiraw_{}_{}_{}_{}_{}.seq".format(inputpara['vendor'].upper(), inputpara['tech'].upper() \
                                                         , datetimearr[0], datetimearr[1], inputpara['carr'].upper())
        
        seqoutputtmppath = os.path.join(inputpara['seqdir'], seqoutputtmpfd)
        seqoutputpath = os.path.join(inputpara['seqdir'], seqoutputfd)

        mylog.info('prepare temp folder {} for creating sequence file ...'.format(seqoutputtmppath))
        if os.path.isdir(seqoutputtmppath):
            util.removeDir(seqoutputtmppath)
        try:
            os.mkdir(seqoutputtmppath)
        except:
            mylog.error('unable to create temp folder {} for creating sequence file'.format(seqoutputtmppath))
            mylog.debug(getException())
            continue

        cmd = "spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos \
--executor-memory 512M --driver-memory 512M --total-executor-cores 1 {} {} {} {} \"{}\" \"{}\" "\
    .format(createseqfilepy, inputpara['vendor'].upper(), inputpara['tech'], inputpara['carr'], setfdpathlock, seqoutputtmppath)
        mylog.info('spark command: {}'.format(cmd))

        # sleep 3 seconds, avoid submitting spark job to mesos too fast
        time.sleep(3)

    if not bhasset:
        mylog.info('no set folder found {}'.format(os.path.join(inputpara['setdir'], setfdpattern)))
        
    return 0
    
if __name__ == '__main__':

    # logger
    dirroot = os.path.dirname(os.path.realpath(__file__))
    logpath = os.path.join(dirroot, "..", "..", "log", "createseqfile")
        
    logfile = os.path.join(logpath, "seqcreator.log")
    mylog = createLogger(logfile)

    mylog.info('============ seqcreator ============')
    
    start = datetime.datetime.now()
    ret = main(mylog)
    done = datetime.datetime.now()
    
    elapsed = done - start
    util.logMessage("Process Time : {}".format(elapsed))

    os._exit(ret)


    
