#!/usr/bin/python
import os
import sys
import time
import uuid

import requests



# set some key var
curr_py_path = os.path.realpath(__file__) # current running file - abs path
curr_py_dir, curr_py_filename = os.path.split(curr_py_path)  # current file and folder - abs path
#curr_py_dir = os.path.dirname(curr_py_path)


# argv[1] - starting file num
# argv[2] - output dir

## globals
prev_jobname = ""

# starting file
filenum = 0
# update starting file
if len(sys.argv) >= 2:
   filenum = int(sys.argv[1])


# output dir
output_dir = ""
if len(sys.argv) >= 3:
   output_dir = sys.argv[2]
output_dir = output_dir.rstrip('/')
if output_dir == "":
   output_dir = "." # default current folder
elif not os.path.isdir(output_dir): # create if not exist
   try:
      os.mkdir(output_dir)
   except:
      print '[%s] Failed to create folder \"%s\"!' % (
        time.strftime("%Y-%m-%d %H:%M:%S"), output_dir)
      print "[%s] Process terminated." % (
        time.strftime("%Y-%m-%d %H:%M:%S"))
      sys.exit(2)
else:
   pass



## Constants
retry_min = 2 # min to retry when all cores are busy/used
core_per_job = 2 # core per job - parsing 1 seq file
check_interval_sec = 12 # sec to check inbetween submit of new job
max_check_ctr = 1 # max num of recheck when there is just 1 job slot left
max_num_job = 6 # max num of job allow concurrently
max_num_job_hardlimit = 6 # max num of job (hard limit)



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

# get current job status
def getCurrJobs(statusJSON, prev_jobname):

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


def canStartNewJob(statusJSON):
	bHaveResource = True
	delay_min = retry_min
	global prev_jobname

	# get cores used
	cores_max, cores_used = getCoresUsed(statusJSON)
 
	# get current job status
	numJobs, numWaitingJobs, bFoundLastSubmit = getCurrJobs(statusJSON, prev_jobname)
	# reset prev job if found
	if bFoundLastSubmit:
		prev_jobname = ""

	# get current worker resource status
	bHaveWorkersResource = haveWorkersResource(statusJSON)
	
	# re-calc max num jobs
	max_num_job = int(cores_max / core_per_job)
	if max_num_job > max_num_job_hardlimit: # check against hard limit
		max_num_job = max_num_job_hardlimit



	# case 1: cannot get job info
	if numJobs == -1 or numWaitingJobs == -1:
		bHaveResource = False
		print "[%s] cannot get jobs info, retry again in %d min" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), delay_min)
		sys.stdout.flush()
	
	# case 2: last submitted job not show up yet
	elif prev_jobname != "" and not bFoundLastSubmit:
		bHaveResource = False
		delay_min = check_interval_sec / 60.0 # only wait for little before update
		print "[%s] last job submit (%s) not completed, retry again in %d sec" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), prev_jobname, delay_min*60)
		sys.stdout.flush()

	# case 3: allowed cores exceed
	elif cores_used > (cores_max - core_per_job):
		bHaveResource = False
		print "[%s] cores exceeding limit, retry again in %d min" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), delay_min)
		sys.stdout.flush()

	# case 4: already have waiting job
	elif numWaitingJobs > 0:
		bHaveResource = False
		print "[%s] number of waiting job = %d, retry again in %d min" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), numWaitingJobs, delay_min)
		sys.stdout.flush()

	# case 5: max job allowed reached
	elif numJobs >= max_num_job:
		bHaveResource = False
		print "[%s] reached max num of job (%d/%d), retry again in %d min" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), numJobs, max_num_job, delay_min)
		sys.stdout.flush()

	# case 6: all worker occupied - either no avail core or no avail mem on all the workers
	elif bHaveWorkersResource == False:
		bHaveResource = False
		print "[%s] all workers are occupied, retry again in %d min" % (
             		time.strftime("%Y-%m-%d %H:%M:%S"), delay_min)
		sys.stdout.flush()


	return bHaveResource, delay_min



def worker():
	"""worker function"""
	global prev_jobname
	jobname = "parse_set_%03d" % (filenum)

	#id = str(uuid.uuid4())
	print "[%s] Task %s start..." % (
		time.strftime("%Y-%m-%d %H:%M:%S"), jobname)

	# submit new job - xml parser
	exec_str = "spark-submit --master spark://master:7077 --executor-memory 1g --driver-memory 512m --total-executor-cores 2 %s/kpi_parser_eric.py ericsson_umts_demo/set_%03d \"%s\" \"%s\" &" % (curr_py_dir, filenum, jobname, output_dir)
	print "[%s] %s" % (
		time.strftime("%Y-%m-%d %H:%M:%S"), exec_str)

	# update prev jobname
	prev_jobname = jobname

	sys.stdout.flush()
	os.system(exec_str)

	return




#######################################################################################
# main proc ###########################################################################


print "[%s] multi process started" % (
        time.strftime("%Y-%m-%d %H:%M:%S"))




check_ctr = 0
while (1):

   # no more file
   if filenum > 200:
      break
     
   try:

      # get status
      statusJSON = getStatusJSON()

      # get cores used
      cores_max, cores_used = getCoresUsed(statusJSON)

      print "[%s] current cores used: %d/%d" % (
              time.strftime("%Y-%m-%d %H:%M:%S"), cores_used, cores_max)
      sys.stdout.flush()


      bStartNewJob, delay_min = canStartNewJob(statusJSON)
      if bStartNewJob == False:
         check_ctr = 0 # reset counter
         time.sleep(delay_min*60)
         continue

      # do last check before adding last available job slot
      if (cores_used == (cores_max - core_per_job)) and (check_ctr < max_check_ctr):
         print "[%s] cores close to limit, retry again in %d sec" % (
             time.strftime("%Y-%m-%d %H:%M:%S"), check_interval_sec*2)
         sys.stdout.flush()
         check_ctr += 1
         time.sleep(check_interval_sec*2)
         continue
      else: # second time checking - last slot still available, initiate new job
         check_ctr = 0 # reset counter


      worker()

      # wait some sec before next task
      time.sleep(check_interval_sec)

      filenum += 1

   except IOError as e:
      print "I/O error({0}): {1}".format(e.errno, e.strerror)
   except ValueError:
      print "Could not convert data to an integer."
   #except:
   #   print "Unexpected error:", sys.exc_info()[0]


print "[%s] multi process ended" % (
	time.strftime("%Y-%m-%d %H:%M:%S"))


sys.stdout.flush()
exit(0)
