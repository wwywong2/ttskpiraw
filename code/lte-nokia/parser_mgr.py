#!/usr/bin/python
import os
import sys
import time
import uuid
import json
import glob
import shutil # move file

import requests
import util



## Constants
new_job_delay_sec = 3 # 12 # sec to check inbetween submit of new job
prev_job_wait_delay_sec = 3 # sec to wait for previous job to show up
general_retry_delay_sec = 4 # sec to retry when all cores are busy/used
core_close_to_limit_delay_sec = 6 # sec to wait when tasks almost used up all cores

core_per_job = 2 # core per job - parsing 1 seq file
max_check_ctr = 1 # max num of recheck when there is just 1 job slot left
max_num_job = 6 # max num of job allow concurrently
max_num_job_hardlimit = 20 # max num of job (hard limit)



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path
#curr_py_dir = os.path.dirname(curr_py_path)

## globals
prev_jobname = ""
check_ctr = 0

# argv[1] - input dir
# argv[2] - output dir
# argv[3] - option json string (optional); 
#           default
#            '{
#              "tech" : "lte", 
#              "vendor" : "eric",
#              "zkStr" : "zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos"
#              "master" : "mesos_master_01", 
#              "masterPort" : 5050,
#              "dispatcherPort" : 7077,
#              "fs" : "mesos_fs_01",
#              "newJobDelay" : 3,
#              "prevJobDelay" : 3,
#              "genRetryDelay" : 4,
#              "closeToLimitDelay" : 6,
#              "exec_core_per_job" : 2,
#              "drvr_mem" : "512m",
#              "exec_mem" : "966m"
#             }'
##           "master":null --> None in python (no coalesce)
##           "overwrite":false/true --> False/True in python
# argv[4] - (optional) "cluster" or "client" mode


if len(sys.argv) < 4:
   util.logMessage("Error: param incorrect.")
   sys.exit(2)

# argv[3] - option json - get first to get all options
optionJSON = ""
if len(sys.argv) > 3:
   optionJSON = sys.argv[3]
if optionJSON == "":
   optionJSON = '{"master":"", "masterPort":5050}'
try:
   optionJSON = json.loads(optionJSON)
except Exception as e: # error parsing json
   optionJSON = '{"master":"", "masterPort":5050}'
   optionJSON = json.loads(optionJSON) 

# default val if not exist
if 'tech' not in optionJSON:
   optionJSON[u'tech'] = "lte"
optionJSON[u'tech'] = optionJSON[u'tech'].lower()   
optionJSON[u'techUp'] = optionJSON[u'tech'].upper()   
if 'vendor' not in optionJSON:
   optionJSON[u'vendor'] = "eric"
optionJSON[u'vendor'] = optionJSON[u'vendor'].lower()   
optionJSON[u'vendorUp'] = optionJSON[u'vendor'].upper()   
if 'zkStr' not in optionJSON:
   optionJSON[u'zkStr'] = "zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos"
if 'master' not in optionJSON:
   optionJSON[u'master'] = ""
if 'masterPort' not in optionJSON:
   optionJSON[u'masterPort'] = 5050
if 'dispatcherPort' not in optionJSON:
   optionJSON[u'dispatcherPort'] = 7077
if 'fs' not in optionJSON:
   optionJSON[u'fs'] = "mesos_fs_01"
if 'newJobDelay' not in optionJSON:
   optionJSON[u'newJobDelay'] = 3
if 'prevJobDelay' not in optionJSON:
   optionJSON[u'prevJobDelay'] = 3
if 'genRetryDelay' not in optionJSON:
   optionJSON[u'genRetryDelay'] = 4
if 'closeToLimitDelay' not in optionJSON:
   optionJSON[u'closeToLimitDelay'] = 6
if 'exec_core_per_job' not in optionJSON:
   optionJSON[u'exec_core_per_job'] = 2
if 'drvr_mem' not in optionJSON:
   optionJSON[u'drvr_mem'] = "512m"
if 'exec_mem' not in optionJSON:
   if optionJSON[u'vendor'] == 'nokia': # nokia need 512m, eric need 966m
      optionJSON[u'exec_mem'] = "512m"
   else:
      optionJSON[u'exec_mem'] = "966m"

# update master info
# logic: if master provided, ignore zkStr and set master
#        else if zkStr provided, use it to find master
#        else if zkStr empty, use default zkStr (zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos) to find master
#        if still cannot find master, use default (mesos_master_01)
if optionJSON[u'master'] != '': # if master defined, not using zookeeper
   optionJSON[u'zkStr'] = ''
   util.logMessage("Master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
else: # if master not defined, use zookeeper
   if optionJSON[u'zkStr'] != '':
      util.logMessage("Try to determine master using zookeeper string: %s" % optionJSON[u'zkStr'])
      master, masterPort = util.getMesosMaster(optionJSON[u'zkStr'])
   else:
      util.logMessage("Try to determine master using default zookeeper string: %s" % 
		"zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos")
      master, masterPort = util.getMesosMaster()
   if master == '': # master not found through zookeeper
      optionJSON[u'master'] = "mesos_master_01"
      util.logMessage("Cannot get master from zookeeper; master default at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))
   else: # master found through zookeeper
      optionJSON[u'master'] = master
      optionJSON[u'masterPort'] = masterPort
      util.logMessage("Master detected at %s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort']))

# pretty print option JSON
util.logMessage("Process start with option:\n%s" % json.dumps(optionJSON, sort_keys=True, indent=3))





# create lock
lockpath = '/tmp/parser_mgr_%s_%s.lock' % (optionJSON[u'vendor'], optionJSON[u'tech'])
try:
   os.makedirs(lockpath)
   util.logMessage("Created lock %s" % lockpath)
except OSError:
   util.logMessage("Found exeisting lock %s, quit process." % lockpath)
   sys.exit(0)





# argv[1] - input dir
input_dir = sys.argv[1]
input_dir = input_dir.rstrip('/')
if not os.path.isdir(input_dir):
   util.logMessage("Failed to open input location \"%s\"!" % input_dir)
   util.logMessage("Process terminated.")
   util.endProcess(lockpath, 2)

# create staging (if not exist)
staging_dir = input_dir+'/staging'
if not os.path.isdir(staging_dir): # create if not exist
   try:
      os.mkdir(staging_dir)
   except:
      util.logMessage("Failed to create folder \"%s\"!" % staging_dir)
      util.logMessage("Process terminated.")
      util.endProcess(lockpath, 2)

# argv[2] - output dir
output_dir = sys.argv[2]
output_dir = output_dir.rstrip('/')
if not os.path.isdir(output_dir):
   util.logMessage("Failed to open output location \"%s\"!" % output_dir)
   util.logMessage("Process terminated.")
   util.endProcess(lockpath, 2)

# argv[3] - option json - done in the beginning...
# set new delay variables
new_job_delay_sec = optionJSON[u'newJobDelay'] 
prev_job_wait_delay_sec = optionJSON[u'prevJobDelay']
general_retry_delay_sec = optionJSON[u'genRetryDelay']
core_close_to_limit_delay_sec = optionJSON[u'closeToLimitDelay']

# argv[4] - process mode
proc_mode = ''
if len(sys.argv) > 4:
   proc_mode = sys.argv[4]
proc_mode = proc_mode.lower()
if not proc_mode == 'cluster':
   proc_mode = 'client'


# update core per job (if it's cluster need more core)
if proc_mode == 'cluster': # cluster mode need one more core
   extra_core_per_job = 1
else:
   extra_core_per_job = 0
core_per_job = core_per_job + extra_core_per_job







# get status JSON
def getStatusJSON():
	js = {}

	resp = requests.get('http://10.26.127.51:8080/json/')
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js

# get status JSON
def getStatusJSON_mesos():
	js = {}

	#resp = requests.get('http://mesos_master_01:5050/tasks')
	#resp = requests.get('http://mesos_master_01:5050/state')
	#resp = requests.get('http://10.26.126.202:5050/state-summary')
	resp = requests.get("http://%s:%d/master/state-summary" % (optionJSON[u'master'], optionJSON[u'masterPort']))
	if resp.status_code != 200:
		# This means something went wrong.
		#raise ApiError('GET /tasks/ {}'.format(resp.status_code))
		pass
	else:
		js = resp.json()

	return js

# get cores used
def getCoresUsed(statusJSON):

	cores = 8 # default to max used already
	if len(statusJSON) == 0:
		# This means something went wrong.
		pass
	else:
		maxcores = int(statusJSON['cores'])
		cores = int(statusJSON['coresused'])

	return maxcores, cores

# get cores used
def getCoresUsed_mesos(statusJSON):

	maxcores = 8
	cores = 8 # default to max used already
	if len(statusJSON) == 0:
		# This means something went wrong.
		pass
	else:
		maxcores = 0
		cores = 0
		slaves = statusJSON['slaves']
		for slave in slaves:
			maxcores += int(slave['resources']['cpus'])
			cores += int(slave['used_resources']['cpus'])

	return maxcores, cores

# get current job status
def getCurrJobs(statusJSON):

	global prev_jobname

	numJobs = 0
	numWaitingJobs = 0
	bFoundLastSubmit = False
	if len(statusJSON) == 0:
		return -1, -1, False
	else:
		jobsArr = statusJSON['activeapps']
		numJobs = len(jobsArr)
		for job in jobsArr:
			if job["state"].upper() == 'WAITING':
				numWaitingJobs += 1
			if job["name"] == prev_jobname:
				bFoundLastSubmit = True
				prev_jobname = "" # reset prev job if found

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current job status
def getCurrJobs_mesos(statusJSON):

	global prev_jobname

	numJobs = 0
	numWaitingJobs = 0
	t_staging = 0
	t_starting = 0
	t_running = 0
	t_killing = 0
	bFoundLastSubmit = False
	if len(statusJSON) == 0:
		return -1, -1, False
	else:
		jobsArr = statusJSON['frameworks']
		for job in jobsArr:
			if (job['name'].upper().find('MARATHON') == -1 and 
				job['name'].upper().find('CHRONOS-') == -1 and
				job['name'].upper().find('SPARK CLUSTER') == -1):
				numJobs += 1
				# further check for waiting task
				if (job['active'] is True and 
					job['TASK_STAGING'] == 0 and
					job['TASK_STARTING'] == 0 and
					job['TASK_RUNNING'] == 0 and
					job['TASK_KILLING'] == 0 and
					job['TASK_FINISHED'] == 0 and
					job['TASK_KILLED'] == 0 and
					job['TASK_FAILED'] == 0 and
					job['TASK_LOST'] == 0 and
					job['TASK_ERROR'] == 0 and
					job['used_resources']['cpus'] == 0):
					numWaitingJobs += 1
			if job['name'] == prev_jobname:
				bFoundLastSubmit = True
				prev_jobname = "" # reset prev job if found

		slaves = statusJSON['slaves']
		for worker in slaves:
			t_staging += int(worker["TASK_STAGING"])
			t_starting += int(worker["TASK_STARTING"])
			t_running += int(worker["TASK_RUNNING"])
			t_killing += int(worker["TASK_KILLING"])
		# that should be = numJobs in all slaves so not returning
		numRunningJobs = t_staging + t_starting + t_running + t_killing 

	return numJobs, numWaitingJobs, bFoundLastSubmit

# get current worker status
def haveWorkersResource(statusJSON):

	bWorkerResource = False
	nNoResource = 0
	if len(statusJSON) == 0:
		return bWorkerResource
	else:
		workersArr = statusJSON['workers']
		numWorkers = len(workersArr)
		for worker in workersArr:
			if worker["coresfree"] == 0 or worker["memoryfree"] == 0:
				nNoResource += 1
		if nNoResource == numWorkers:
			bWorkerResource = False
		else:
			bWorkerResource = True

	return bWorkerResource

# get current worker status
def haveWorkersResource_mesos(statusJSON):

	bWorkerResource = False
	nNoResource = 0
	if len(statusJSON) == 0:
		return bWorkerResource
	else:
		slaves = statusJSON['slaves']
		numWorkers = len(slaves)
		for worker in slaves:
			if worker["resources"]["cpus"] == worker["used_resources"]["cpus"] or worker["resources"]["mem"] == worker["used_resources"]["mem"]:
				nNoResource += 1
		if nNoResource == numWorkers:
			bWorkerResource = False
		else:
			bWorkerResource = True

	return bWorkerResource

def canStartNewJob(statusJSON):

	bHaveResource = True
	delay_sec = general_retry_delay_sec # general retry delay
	global prev_jobname
	global check_ctr

	# get status
	statusJSON = getStatusJSON_mesos()

	# get cores used
	cores_max, cores_used = getCoresUsed_mesos(statusJSON)
	util.logMessage("Current cores used: %d/%d" % (cores_used, cores_max))
 
	# get current job status
	numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON)

	# get current worker resource status
	bHaveWorkersResource = haveWorkersResource_mesos(statusJSON)
	
	# re-calc max num jobs
	max_num_job = int(cores_max / core_per_job)
	if max_num_job > max_num_job_hardlimit: # check against hard limit
		max_num_job = max_num_job_hardlimit



	# case 1: cannot get job info
	if numJobs == -1 or numWaitingJobs == -1:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("cannot get jobs info, retry again in %d sec" % delay_sec)

		'''
	# case 2: last submitted job not show up yet
	elif prev_jobname != "" and not bFoundLastSubmit:
		bHaveResource = False
		delay_sec = prev_job_wait_delay_sec # only wait for little before update
		util.logMessage("last job submit: %s not completed, retry again in %d sec" % (prev_jobname, delay_sec))
		'''

	# case 3: allowed cores exceed
	elif cores_used > (cores_max - core_per_job):
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("cores exceeding limit, retry again in %d sec" % delay_sec)

	# case 4: do last n # of check before adding last available job slot
	# check_ctr == max_check_ctr means already check n # of times, pass test
	elif cores_used == (cores_max - core_per_job):
		if check_ctr < max_check_ctr:
			check_ctr += 1
			bHaveResource = False
			delay_sec = core_close_to_limit_delay_sec
			util.logMessage("cores close to limit, retry again in %d sec" % (delay_sec))
		else:
			check_ctr = 0 # condition met, reset retry counter

	# case 5: more than 1 waiting job
	elif numWaitingJobs > 1:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("number of waiting job = %d, retry again in %d sec" % (numWaitingJobs, delay_sec))

	# case 6: max job allowed reached
	elif numJobs >= max_num_job:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("reached max num of job (%d/%d), retry again in %d sec" % (numJobs, max_num_job, delay_sec))

	# case 7: all worker occupied - either no avail core or no avail mem on all the workers
	elif bHaveWorkersResource == False:
		bHaveResource = False
		check_ctr = 0 # reset retry counter
		util.logMessage("all workers are occupied, retry again in %d sec" % delay_sec)



	return bHaveResource, delay_sec


# worker function
def worker(seqfile):

	global prev_jobname
        seqfile_dir, seqfile_file = os.path.split(seqfile)
	jobname = seqfile_file
	jobname = jobname.replace(' ', '-') # for cluster mode, job name should not contain space - spark bug

	util.logMessage("Task %s start..." % jobname)

	# create master string
	if proc_mode == 'cluster': # assume the leading master that zk return is the one to be use for dispatcher
		exec_str_master = "mesos://%s:%d" % (optionJSON[u'master'], optionJSON[u'dispatcherPort'])
	else: # client
		if optionJSON[u'zkStr'] != '':
			exec_str_master = "mesos://%s" % (optionJSON[u'zkStr'])
		else:
			exec_str_master = "mesos://%s:%d" % (optionJSON[u'master'], optionJSON[u'masterPort'])

	# create spark string
        exec_str_spark = "/opt/spark/bin/spark-submit \
--master %s \
--deploy-mode %s \
--driver-memory %s \
--executor-memory %s \
--total-executor-cores %d" % (
		exec_str_master,
		proc_mode,
		optionJSON[u'drvr_mem'],
		optionJSON[u'exec_mem'],
		optionJSON[u'exec_core_per_job'])
	if proc_mode == 'cluster': # cluster have more options to be set
		exec_str_spark += " --py-files \"%s,%s,%s\"" % (
			"file://%s/util.py" % curr_py_dir,
			"file://%s/xmlparser_%s_%s.py" % (curr_py_dir, optionJSON[u'tech'], optionJSON[u'vendor']),
			"file://%s/config.ini" % curr_py_dir)

	# create python string
	exec_str_py = "%s/kpi_parser_%s_%s.py" % (curr_py_dir, optionJSON[u'tech'], optionJSON[u'vendor'])
	if proc_mode != 'cluster': # client - support multi master (zookeeper)
		exec_str_app = "%s \"%s\" %s \"imnosrf@%s|%s\" \"%s\" &" % (
			exec_str_py, jobname, seqfile, optionJSON[u'fs'], output_dir, proc_mode)
	else: # cluster - currently not support multi master (zookeeper)
		exec_str_app = "%s \"%s\" %s \"imnosrf@%s\|%s\" \"%s\"" % (
			exec_str_py, jobname, seqfile, optionJSON[u'fs'], output_dir, proc_mode)

	exec_str = exec_str_spark + " " + exec_str_app

	'''
	# old samples
	# submit new job - xml parser
	#exec_str = "spark-submit --master spark://master:7077 --executor-memory 512m --driver-memory 512m --total-executor-cores 2 %s/kpi_parser_eric.py \"%s\" %s \"%s\" &" % (curr_py_dir, jobname, seqfile, output_dir)
	if proc_mode != 'cluster': # client - support multi master (zookeeper)
	#	exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:5050 --driver-memory 512m --executor-memory 966m --total-executor-cores 2 %s/kpi_parser_lte_eric.py \"%s\" %s \"tts@mesos_fs_01|%s\" \"client\" &" % (curr_py_dir, jobname, seqfile, output_dir)
		exec_str = "/opt/spark/bin/spark-submit --master mesos://zk://mesos_master_01:2181,mesos_master_02:2181,mesos_master_03:2181/mesos --driver-memory 512m --executor-memory 966m --total-executor-cores 2 %s/kpi_parser_lte_eric.py \"%s\" %s \"imnosrf@mesos_fs_01|%s\" \"client\" &" % (curr_py_dir, jobname, seqfile, output_dir)
	else: # cluster - currently not support multi master (zookeeper)
	#	exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:7077 --deploy-mode cluster --driver-memory 512m --executor-memory 966m --total-executor-cores 2 --py-files \"file:///home/tts/ttskpiraw/code/lte-eric/util.py,file:///home/tts/ttskpiraw/code/lte-eric/xmlparser_lte_eric.py,file:///home/tts/ttskpiraw/code/lte-eric/config.ini\" %s/kpi_parser_lte_eric.py \"%s\" %s \"tts@mesos_fs_01\|%s\" \"cluster\"" % (curr_py_dir, jobname, seqfile, output_dir)
		exec_str = "/opt/spark/bin/spark-submit --master mesos://mesos_master_01:7077 --deploy-mode cluster --driver-memory 512m --executor-memory 966m --total-executor-cores 2 --py-files \"file:///home/imnosrf/ttskpiraw/code/lte-eric/util.py,file:///home/imnosrf/ttskpiraw/code/lte-eric/xmlparser_lte_eric.py,file:///home/imnosrf/ttskpiraw/code/lte-eric/config.ini\" %s/kpi_parser_lte_eric.py \"%s\" %s \"imnosrf@mesos_fs_01\|%s\" \"cluster\"" % (curr_py_dir, jobname, seqfile, output_dir)
	'''

	util.logMessage("%s" % exec_str)

	# update prev jobname
	prev_jobname = jobname

	os.system(exec_str)

	return




#######################################################################################
# main proc ###########################################################################
def main(input_dir, optionJSON):

   '''
   # sameple code
   # get status
   statusJSON = getStatusJSON_mesos()
   cores_max, cores_used = getCoresUsed_mesos(statusJSON)
   print 'max:%s, used:%s' % (cores_max, cores_used)
   print 'have resource: %s' % haveWorkersResource_mesos(statusJSON)
   numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs_mesos(statusJSON, '1x2c_client')
   print 'numJobs: %s; numWaitingJobs: %s; bFoundLastSubmit: %s' % (numJobs, numWaitingJobs, bFoundLastSubmit)
   exit(0)
   '''

   # go thru all seq file/folder
   if optionJSON[u'vendorUp'] == 'ERIC':
      vendorFULL = 'ERICSSON'
   else:
      vendorFULL = 'NOKIA'
   inputSeqPath = input_dir+"/ttskpiraw_%s_%s_*_TMO*.seq" % (vendorFULL, optionJSON[u'techUp'])
   inputSeqList = glob.glob(inputSeqPath)
   if len(inputSeqList) <= 0:  # no file
      util.logMessage("No seq file to process: %s" % inputSeqPath)
      util.endProcess(lockpath, 0)

   # move seq file into staging first to prevent other proc from touching them
   inputSeqStageList = []
   for curr_file in sorted(inputSeqList):
      util.logMessage("Moving %s to staging dir %s" % (curr_file, staging_dir))
      try:
         shutil.move(curr_file, staging_dir)
         curr_filedir, curr_filename = os.path.split(curr_file)
         inputSeqStageList.append(os.path.join(staging_dir,curr_filename))
      except shutil.Error as e:
         util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
      except:
         util.logMessage("Unexpected error")

   # going to each seq in the staging area and submit process
   for curr_file in inputSeqStageList:
      try:

         # get status
         statusJSON = getStatusJSON_mesos()
         bStartNewJob, delay_sec = canStartNewJob(statusJSON)
         while (bStartNewJob == False):
            time.sleep(delay_sec)
            bStartNewJob, delay_sec = canStartNewJob(statusJSON) # retest after the sleep

         # process file
         worker(curr_file)

         # wait some sec before next task
         time.sleep(new_job_delay_sec)
         
      except Exception as e:
         util.logMessage("Error: failed to process file %s\n%s" % (curr_file, e))
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")
      except:
         util.logMessage("Unexpected error")
         # try to move it back to input dir for re-processing next round
         try:
            shutil.move(curr_file, input_dir)
         except shutil.Error as e:
            util.logMessage("Error: failed to move file %s\n%s" % (curr_file, e))
         except:
            util.logMessage("Unexpected error")


   return 0






if __name__ == "__main__":

   # Execute Main functionality
   util.logMessage("multi process started")
   ret = main(input_dir, optionJSON)
   util.logMessage("multi process ended")
   util.endProcess(lockpath, ret)


