#!/usr/bin/python
import os, shutil, sys, struct, datetime
import json, util, uuid, linecache, glob
import subprocess, gzip, logging
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

def testGZFile2(gzfile):
    bgoodgz = True
    with gzip.open(gzfile) as g:
        try:
            while g.read(1024 * 1024):
                pass
        except IOError as e:
            bgoodgz = False
        finally:
            return bgoodgz

def testGZFile(gzfile):
    cmd = 'gzip -t {}'.format(gzfile)
    ret = subprocessShellExecute(cmd)
    return ret['ret']

def moveInputFilesToWorkFolder(lzdir, wpath, mylog):
    wrokfilelist = []
    for folderName, subfolders, filenames in os.walk(lzdir):        
        for f in filenames:
            srcfile = os.path.abspath(os.path.join(folderName, f))
            if srcfile.endswith('.gz'):
                cmd = "lsof | grep {}".format(srcfile)
                ret = subprocessShellExecute(cmd)
                isfileopened = ret['ret']
                if not isfileopened:
                    isgoodgz = testGZFile2(srcfile)
                    #isgoodgz = testGZFile(srcfile)
                    if isgoodgz:
                        workfile = os.path.join(wpath, f)
                        try:
                            shutil.move(srcfile, workfile)
                        except:
                            mylog.error('failed to move {} to {}'.format(srcfile, workdir))
                            mylog.debug(getException())
                            continue
                        wrokfilelist.append(workfile)
                    else:
                        mylog.error('gz file corrupt: {}'.format(srcfile))

    # avoid same file in different input folder
    return list(set(wrokfilelist))

def iterCount(iterobj):
    return sum(1 for item in iterobj)
                           
def getuncompressedsize(filename):
    with open(filename, 'rb') as f:
        f.seek(-4, 2)
        return struct.unpack('I', f.read(4))[0]     

def fileExistInPath(path):
    return any(isfile(os.path.join(path, i)) for i in os.listdir(path))

def endProcess(lockpath, lzdir, workdir, start, mylog):

    # any file left in the work folder will be send to landing zone for next scheduler
    # normal exit will not have any file left
    #
    if fileExistInPath(workdir):
        mylog.info('found files in {}'.format(workdir))
        mylog.info('move {} back to landing zone {}'.format(workdir, lzdir))
        try:
            shutil.move(workdir ,lzdir)
        except:
            mylog.error("move work folder {} to {} failed".format(workdir, lzdir))
            mylog.debug(getException())
            pass
            
    if os.path.isdir(workdir):
        mylog.info('remove work folder {}'.format(workdir))
        try:
            util.removeDir(workdir)
        except:
            mylog.error("remove work folder {} failed".format(workdir))
            mylog.debug(getException())
            pass

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

def createNewSetFolder(inputpara, datetimearr, numset):
    retobj = {}

    workid = 'ttskpiraw_{}_{}_{}_{}_{}_{}'.format(inputpara['vendor'].upper(), inputpara['tech'].upper()\
                                     , datetimearr[0], datetimearr[1], inputpara['carr'].upper(), str(numset).zfill(5))
    # create set directory
    #ERICSSON_LTE_20170608_185049558087_TMO.seqinput.tmp
    setfoldertmp ='{}.seqinput.tmp'.format(workid)
    setfolder ='{}.seqinput'.format(workid)
    setfolderpathtmp = os.path.join(inputpara['setdir'], setfoldertmp)
    setfolderpath = os.path.join(inputpara['setdir'], setfolder)
    if os.path.isdir(setfolderpathtmp):
        util.removeDir(setfolderpathtmp)
    try:
        os.mkdir(setfolderpathtmp)
        retobj['ret'] = True
        retobj['setfoldertmp'] = setfoldertmp
        retobj['setfolder'] = setfolder
        retobj['setfolderpathtmp'] = setfolderpathtmp
        retobj['setfolderpath'] = setfolderpath
        retobj['msg'] = ''
    except:
        retobj['ret'] = False
        retobj['msg'] = getException()
    finally:
        return retobj

def createPreSeqFolder(inputpara, inputfilelist, datetimearr, mylog):

    wronginputfolder = os.path.join(inputpara['setdir'], 'wrong_size')
    if not os.path.isdir(wronginputfolder):
        try:
            mylog.info("creating folder for bakcup wrong/big size gz ...".format(wronginputfolder))
            os.mkdir(wronginputfolder)
        except:
            mylog.error("create folder for backup wrong input failed: {}".format(wronginputfolder))
            mylog.debug(getException())
            return 1

    setsize = 0
    numset = 1
    bhastmpfolder = False
    setfolderobj = {}
    
    # check any tmp folder left in inputpara['setdir'], normally atmost 1 tmp folder
    tmppreseqfolderpath = os.path.join(inputpara['setdir'], "*.tmp")
    for tmpfolderpath in glob.glob(tmppreseqfolderpath):
        bhastmpfolder = True
        tmpfolder = os.path.dirname(tmpfolderpath)
        setfolderobj['setfoldertmp'] = tmpfolder
        setfolderobj['setfolder'] = tmpfolder.replace(".tmp", "")
        setfolderobj['setfolderpathtmp'] = tmpfolderpath
        setfolderobj['setfolderpath'] = tmpfolderpath.replace(".tmp", "")
        setsize = sum([getuncompressedsize(f) for f in glob.glob(os.path.join(tmpfolderpath, "*.gz")) if os.path.isfile(f)])
        
        mylog.warning("found tmp folder: {} ...".format(tmpfolderpath))
        mylog.warning("tmp folder size: {} bytes...".format(setsize))

        numset = 0
        break

    if not bhastmpfolder:
        mylog.info("creating new set folder({}) ...".format(numset))
        setfolderobj = createNewSetFolder(inputpara, datetimearr, numset)
        if not setfolderobj['ret']:
            mylog.error("create new set folder({}) failed".format(numset))
            mylog.debug(setfolderobj['msg'])
            return 1
        mylog.debug("new set folder is created: {}".format(setfolderobj))
    
    # 256 MB = 268,435,456 Bytes
    ret = 0
    for fn in inputfilelist:
        if not os.path.isfile(fn):
            mylog.error("{} does not exit, the file probably has been moved already".format(fn))
            continue
        
        fnsize = getuncompressedsize(fn)
        if (268000000 < fnsize ): # big file size over 256 MB
            mylog.warning("found big size gz: {} {}...".format(fn, fnsize))
            try:      
                shutil.move(fn, wronginputfolder)
            except:
                mylog.error("moving issue file {} to {} failed".format(fn, wronginputfolder))
                mylog.debug(getException())
            else:
                mylog.info("move issue file {} to {}".format(fn, wronginputfolder))
                continue

        setsize = setsize + fnsize
        #print fnsize, setsize
        if (268000000 < setsize ):
            # rename tmp folder to formal folder for sequence file creator
            try:
                mylog.info("tmp set folder {} is full: {}, max: 268000000".format(setfolderobj['setfolderpathtmp'], setsize - fnsize))
                mylog.info("rename {} to {}".format(setfolderobj['setfolderpathtmp'], setfolderobj['setfolderpath'])) 
                os.rename(setfolderobj['setfolderpathtmp'], setfolderobj['setfolderpath'])
            except:
                mylog.error("rename folder {} to {} failed".format(setfolderobj['setfolderpathtmp'], setfolderobj['setfolderpath']))
                mylog.debug(getException())
                ret = 1
                break

            # create new set folder
            setsize = fnsize
            numset += 1
            mylog.info("creating new set folder({}) ...".format(numset))
            setfolderobj = createNewSetFolder(inputpara, datetimearr, numset)
            if not setfolderobj['ret']:
                mylog.error("create new set folder({}) failed".format(numset))
                mylog.debug(setfolderobj['msg'])
                ret = 1
                break
            mylog.debug("new set folder is created: {}".format(setfolderobj))

        try:            
            shutil.move(fn, setfolderobj['setfolderpathtmp'])
        except:
            mylog.error('move file {} to {} failed'.format(fn, setfolderobj['setfolderpathtmp']))
            mylog.debug(getException())
            continue

    return ret

def main():

    '''
    python filedistributor.py ERICSSON LTE TMO /home/imnosrf/ttskpiraw/landingzone/lte-nokia /mnt/nfskpi/wyang/ttskpiraw/lte-nokia/input
    '''
    inputpara = {}
    try:
        inputpara['vendor'] = sys.argv[1]
        inputpara['tech'] = sys.argv[2]
        inputpara['carr'] = sys.argv[3]
        inputpara['lzdir'] = sys.argv[4]
        inputpara['setdir'] = sys.argv[5]
    except:
        print 'Incorrect parameters'
        print '{}'.format(getException())
        return 1

    # create lock
    lockpath = '/tmp/filedistributor_{}_{}.lock'.format(inputpara['vendor'].lower(), inputpara['tech'].lower())
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
    logfile = os.path.join(logpath, "filedistributor_{}_{}.log".format(inputpara['tech'].lower(), inputpara['vendor'].lower()))
    
    mylog = createLogger(logfile)
    mylog.info('============ filedistributor ============')
    start = datetime.datetime.now()

    mylog.info('lock path: {}'.format(lockpath))
    
    uid = str(uuid.uuid1())
    datetimearr = datetime.datetime.now().strftime('%Y%m%d %H%M%S%f').split(' ')
    wfolder = '{}_{}_{}_{}_{}'.format(inputpara['vendor'].upper(), inputpara['tech'].upper()\
                                     , datetimearr[0], datetimearr[1], inputpara['carr'].upper())

    wpath = os.path.join(inputpara['setdir'], wfolder)
    mylog.info('making work directory: {}'.format(wpath))
    if os.path.isdir(wpath):
        util.removeDir(wpath)
    try:
        os.mkdir(wpath)
    except:
        mylog.error("create working directory failed: {}".format(wpath))
        mylog.debug(getException())
        endProcess(lockpath, inputpara['lzdir'], wpath, start, mylog)
        return 1

    mylog.info('start checking/moving input files from {} ...'.format(inputpara['lzdir'])) 
    inputfilelist = moveInputFilesToWorkFolder(inputpara['lzdir'], wpath, mylog)
    inputfiles = len(inputfilelist)
    if inputfiles == 0:
        mylog.warning("no valid input file found in: {}".format(inputpara['lzdir']))

        # check any tmp folder left longer than 10 mins, normally atmost 1 tmp folder
        # if found multiple tmp folders, rename them to formal folder name for seq creator anyway
        mylog.info("searching tmp folder in {} ... ".format(inputpara['setdir']))
        bhastmpfolder = False
        for tmpfd in glob.glob(os.path.join(inputpara['setdir'], "*.tmp")):
            bhastmpfolder = True
            mylog.info("found tmp folder {}: ".format(tmpfd))
            tmpfdname = os.path.basename(tmpfd)
            tmpfdnamearr = tmpfd.split('_')
            try:
                tmpfddatetimestr = '{}_{}'.format(tmpfdnamearr[3], tmpfdnamearr[4])
            except:
                mylog.error("incorrect tmp folder format: {}: ".format(tmpfd))
                mylog.debug(getException())
                continue
            else:
                # compare date time, longer than 10 mins will become normal folder for seq creator
                tmpfddatetimeobj = datetime.datetime.strptime(tmpfddatetimestr, '%Y%m%d_%H%M%S%f')
                nowdatetimeobj = datetime.datetime.now()
                if (nowdatetimeobj - tmpfddatetimeobj).seconds > 600:
                    mylog.warning("tmp folder {} is idle over 600 seconds".format(tmpfd))
                    mylog.info("rename tmp folder {} to {}".format(tmpfd, tmpfd.replace('.tmp', '')))
                    try:
                        os.rename(tmpfd, tmpfd.replace(".tmp", ""))
                    except:
                        mylog.error("renam tmp folder {} to {} failed".format(tmpfd, tmpfd.replace('.tmp', '')))
                        mylog.debug(getException())
                        continue
                  
        endProcess(lockpath, inputpara['lzdir'], wpath, start, mylog)
        return 0
    
    mylog.info('found #input files: {}'.format(inputfiles))

    mylog.info('preparing set folder for sequence file creator ...')
    ret = createPreSeqFolder(inputpara, inputfilelist, datetimearr, mylog)

    endProcess(lockpath, inputpara['lzdir'], wpath, start, mylog)

    return 0
    
if __name__ == '__main__':

    ret = main()
    os._exit(ret)


    
