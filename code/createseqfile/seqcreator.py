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
    fh = handlers.RotatingFileHandler(logfile, maxBytes=(1048576*2), backupCount=7)
    fh.setFormatter(logformat)
    logger.addHandler(fh)

    return logger

################################################
#   subprocessShellExecute
#       1 . MySQL
#       2.  Execuable
################################################
def subprocessShellExecute(cmd, wait = True):
    retObj = {}
    if not wait:
        p = subprocess.Popen(cmd, shell=True, stdin=None, stdout=None, stderr=None, close_fds=True)
        retObj['ret'] = True
        return retObj
    else:
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

def endProcess(lockpath, start, mylog):

    if os.path.isdir(lockpath):
        mylog.info('remove lockpath {}'.format(lockpath))
        try:
            util.removeDir(lockpath)
        except:
            mylog.error("remove lock {} failed".format(lockpath))
            mylog.debug(getException())
            pass

    done = datetime.datetime.now()
    elapsed = done - start
    mylog.info("Process Time : {}".format(elapsed))

def main():

    '''
    python seqcreator.py ERICSSON LTE TMO /mnt/nfskpi/wyang/ttskpiraw/lte-eric/input /mnt/nfskpi/wyang/ttskpiraw/lte-eric/input_seq
    '''
    
    inputpara = {}
    try:
        inputpara['vendor'] = sys.argv[1]
        inputpara['tech'] = sys.argv[2]
        inputpara['carr'] = sys.argv[3]
        inputpara['setdir'] = sys.argv[4]
        inputpara['seqdir'] = sys.argv[5]
    except:
        print 'Incorrect parameters'
        print '{}'.format(getException())
        return 1

    # create lock
    lockpath = '/tmp/seqcreator_{}_{}.lock'.format(inputpara['vendor'].lower(), inputpara['tech'].lower())
    try:
        print 'create lock {}'.format(lockpath)
        os.makedirs(lockpath)
    except OSError:
        print 'found existing lock {}'.format(lockpath)
        print '{}'.format(getException())
        return 9
    
    # logger
    dirroot = os.path.dirname(os.path.realpath(__file__))
    logpath = os.path.join(dirroot, "..", "..", "log")
        
    logfile = os.path.join(logpath, "seqcreator_{}_{}.log".format(inputpara['tech'].lower(), inputpara['vendor'].lower()))
    mylog = createLogger(logfile)

    mylog.info('============ seqcreator ============')
    start = datetime.datetime.now()

    mylog.info('lock path: {}'.format(lockpath))

    # get config json
    configfile = os.path.join(dirroot, "seqsparkconfig.json")
    mylog.info('validating config file: {}'.format(configfile))
    if not os.path.isfile(configfile):
        mylog.error('job configuration file does not exist: {}'.format(configfile))
        endProcess(lockpath, start, mylog)
        return 1
    try:
        config = util.json_reader(configfile)
    except:
        mylog.error('unable to read configuration file')
        mylog.debug(getException())
        endProcess(lockpath, start, mylog)
        return 1

    try:
        sparkmastersarr = config['sparkmasters']
        seqjobconfig = config['seqcreate']
        executormemory = seqjobconfig['executormemory']
        drivermemory = seqjobconfig['drivermemory']
        totalexecutorcore = seqjobconfig['totalexecutorcore']
        if len(sparkmastersarr) <=0 :
            mylog.error('empty spark master list')
            endProcess(lockpath, start, mylog)
            return 1
        
        sparkmasterliststr = ''
        for sparkmaster in sparkmastersarr:
            try:
                ip = sparkmaster['ip']
            except:
                mylog.warning('no ip/name found for master')
                mylog.debug(getException())
                continue
            try:
                port = sparkmaster['port']
            except:
                port = '2181'
                pass
            if sparkmasterliststr == '':
                sparkmasterliststr = '{}:{}'.format(ip, port)
            else:
                sparkmasterliststr += ',{}:{}'.format(ip, port)
    except:
        mylog.error('configuration error')
        mylog.debug(getException())
        endProcess(lockpath, start, mylog)
        return 1

    mylog.info('spark master list: {}'.format(sparkmasterliststr))

    createseqfilepy = os.path.join(dirroot, "createseqfile.py")
    if not os.path.isfile(createseqfilepy):
        mylog.error('createseqfile.py does not exit: {}'.format(createseqfilepy))
        endProcess(lockpath, start, mylog)
        return 1
    mylog.info('createseqfile.py exits: {}'.format(createseqfilepy))

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

        cmd = "nohup spark-submit --master mesos://zk://{}/mesos --executor-memory {} --driver-memory {} --total-executor-cores {} \
{} {} {} {} \"{}\" \"{}\" "\
.format(sparkmasterliststr, executormemory, drivermemory, totalexecutorcore\
        , createseqfilepy, inputpara['vendor'].upper(), inputpara['tech']\
        , inputpara['carr'], setfdpathlock, inputpara['seqdir'])

        mylog.info('spark command: {}'.format(cmd))

        mylog.info('command executing ...')
        #os.system(cmd)
        # use subprocessShellExecute will wait process finish
        ret = subprocessShellExecute(cmd, False)
        if not ret['ret']:
            mylog.error('sequence file creation failed')
            mylog.debug(ret['msg'])

        # sleep 10 seconds, avoid submitting spark job to mesos too fast
        time.sleep(10)

    if not bhasset:
        mylog.info('no set folder found, pattern: {}'.format(os.path.join(inputpara['setdir'], setfdpattern)))

    endProcess(lockpath, start, mylog)
        
    return 0
    
if __name__ == '__main__':
 
    ret = main()
    os._exit(ret)


    
