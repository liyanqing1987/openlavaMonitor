#!PYTHONPATH
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import datetime
import socket
import psutil

sys.path.append('MONITORPATH')
from common import common
from common import openlava_common
from common import sqlite3_common
from conf import config

os.environ["PYTHONUNBUFFERED"]="1"

hostname = socket.gethostname()
sampleTime = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
currentDay = datetime.datetime.today().strftime('%Y%m%d')
currentSeconds = int(time.time())

class jobResourceSample:
    """
    Get job related processes.
    Sample process related cpu and memory information.
    Save data into sqlite3.
    """
    def __init__(self):
        self.dbPath = str(config.dbPath) + '/resource'
        jobDbPath = str(self.dbPath) + '/job'

        if not os.path.exists(jobDbPath):
            try:
                os.system('mkdir -p ' + str(jobDbPath))
            except:
                print('*Error*: Failed on creating sqlite job db directory "' + str(jobDbPath) + '".')
                sys.exit(1)

    def getAllNoRootPids(self):
        print('>>> Getting all no root pids ...')

        pids = []
        command = "ps -U root -u root -N | awk '{print $1}' | grep -v PID"
        (returnCode, stdout, stderr) = common.subprocessPopen(command)
        stdout = str(stdout, 'utf-8')

        for line in stdout.split():
            pids.append(int(line.strip()))

        return(pids)

    def getPidInfos(self, pid, pidProcess=''):
        """
        psutil manuel please see https://blog.csdn.net/zhouzhiwengang/article/details/72461295.
        """
        pidDic = {
                 'hostname' : hostname,
                 'username' : '',
                 'pid' : pid,
                 'ppid' : 0,
                 'create_time' : 0.0,
                 'status' : '',
                 'cpu_percent' : 0.0,
                 'memory' : 0.0,
                 'num_threads' : 1,
                 'name' : '',
                 'cmdline' : [],
                 'children_pids' : [],
                 }

        if str(pidProcess) == '':
            try:
                pidProcess = psutil.Process(pid)
            except Exception as warning:
                print('        *Warning*: ' + str(warning))
                return(pidDic, pidProcess)

        # The process name.
        try:
            pidName = pidProcess.name()
            pidDic['name'] = pidName
        except:
            pass

        # The command line this process has been called with.
        try:
            pidCmdline = pidProcess.cmdline()
            pidDic['cmdline'] = pidCmdline
        except:
            pass

        # The process creation time as a floating point number expressed in seconds since the epoch.
        try:
            pidCreateTime = pidProcess.create_time()
            pidDic['create_time'] = pidCreateTime
        except:
            pass

        # The current process status as a string. The returned string is one of the psutil.STATUS_* constants.
        try:
            pidStatus = pidProcess.status()
            pidDic['status'] = pidStatus
        except:
            pass

        # The name of the user that owns the process. On UNIX this is calculated by using real process uid.
        try:
            pidUsername = pidProcess.username()
            pidDic['username'] = pidUsername
        except:
            pass

        # The number of threads used by this process.
        try:
            pidNumThreads = pidProcess.num_threads()
            pidDic['num_threads'] = pidNumThreads
        except:
            pass

        # The process parent pid.
        try:
            pidPpid = pidProcess.ppid()
            pidDic['ppid'] = pidPpid
        except:
            pass

        # Return the children of this process as a list of Process objects, preemptively checking whether PID has been reused. 
        # If recursive is True return all the parent descendants. Example assuming A == this process:
        # 
        # A ─┐
        #    │
        #    ├─ B (child) ─┐
        #    │             └─ X (grandchild) ─┐
        #    │                                └─ Y (great grandchild)
        #    ├─ C (child)
        #    └─ D (child)
        # 
        # >>> p.children()
        # B, C, D
        # >>> p.children(recursive=True)
        # B, X, Y, C, D
        try:
            pidChildren = pidProcess.children(recursive=True)
            for item in pidChildren:
                pid = item.pid
                pidDic['children_pids'].append(pid)
        except:
            pass

        # Return a float representing the process CPU utilization as a percentage. 
        # The returned value refers to the utilization of a single CPU, i.e. it is not evenly split between the number of available CPU cores. 
        # When interval is > 0.0 compares process times to system CPU times elapsed before and after the interval (blocking). 
        # When interval is 0.0 or None compares process times to system CPU times elapsed since last call, returning immediately.
        # That means the first time this is called it will return a meaningless 0.0 value which you are supposed to ignore. 
        # In this case is recommended for accuracy that this function be called a second time with at least 0.1 seconds between calls.
        try:
            pidCpuPercent = pidProcess.cpu_percent(interval=None)
            cpu = round(pidCpuPercent/100, 4)
            pidDic['cpu_percent'] = cpu
        except Exception as error:
            pass

        # Return a namedtuple with variable fields depending on the platform representing memory information about the process. 
        # The “portable” fields available on all plaforms are rss and vms. All numbers are expressed in bytes.
        # * rss: aka “Resident Set Size”, this is the non-swapped physical memory a process has used. On UNIX it matches “top“‘s RES column (seedoc). 
        #        On Windows this is an alias for wset field and it matches “Mem Usage” column of taskmgr.exe.
        # * vms: aka “Virtual Memory Size”, this is the total amount of virtual memory used by the process. 
        #        On UNIX it matches “top“‘s VIRT column (seedoc). On Windows this is an alias for pagefile field and it matches “Mem Usage” “VM Size” column of taskmgr.exe.
        # * shared: (Linux) memory that could be potentially shared with other processes. This matches “top“‘s SHR column (see doc).
        # * text (Linux, BSD): aka TRS (text resident set) the amount of memory devoted to executable code. This matches “top“‘s CODE column (seedoc).
        # * data (Linux, BSD): aka DRS (data resident set) the amount of physical memory devoted to other than executable code. It matches “top“‘s DATA column (see doc).
        # * lib (Linux): the memory used by shared libraries.
        # * dirty (Linux): the number of dirty pages.
        try:
            pidMemoryInfo = pidProcess.memory_info()
            rss = pidMemoryInfo.rss
            shared = pidMemoryInfo.shared
            memory = round((int(rss)-int(shared))/1024/1024/1024, 4)
            pidDic['memory'] = memory
        except:
            pass

        return(pidDic, pidProcess)

    def getPidsDic(self, pids, pidProcessDic={}):
        print('>>> Getting pid informations ...')

        currentTime = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print('    [' + str(currentTime) + '] Sample start.')

        pidsDic = {}

        for pid in pids:
            if pid in pidProcessDic.keys():
                pidProcess = pidProcessDic[pid]
            else:
                pidProcess = ''

            (pidDic, pidProcess) = self.getPidInfos(pid, pidProcess)
            pidsDic[pid] = pidDic

            if pid not in pidProcessDic.keys():
                pidProcessDic[pid] = pidProcess

        currentTime = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print('    [' + str(currentTime) + '] Sample end.')

        return(pidsDic, pidProcessDic)

    def getJobPidListDicFromProcessTree(self, pidsDic, inputJob):
        jobPidList = []
        lsbatch1Compile = re.compile('^.*/sbin/res .*/[0-9]+\.([0-9]+)\.([0-9+])$')
        lsbatch2Compile = re.compile('^.*/sbin/res .*/[0-9]+\.([0-9]+)$')

        for pid in pidsDic.keys():
            pidDic = pidsDic[pid]
            if pidDic['name'] == 'res':
                pidCommand = pidDic['cmdline']
                pidCommandString = ' '.join(pidCommand)
                pidChildrenPidList = []

                if lsbatch1Compile.match(pidCommandString):
                    myMatch = lsbatch1Compile.match(pidCommandString)
                    jobHead = myMatch.group(1)
                    jobTail = myMatch.group(2)
                    job = str(jobHead) + '[' + str(jobTail) + ']'
                    if job == inputJob:
                        jobPidList = [pid,]
                        pidChildrenPidList = pidsDic[pid]['children_pids']
                        jobPidList.extend(pidChildrenPidList)
                elif lsbatch2Compile.match(pidCommandString):
                    myMatch = lsbatch2Compile.match(pidCommandString)
                    job = myMatch.group(1)
                    if job == inputJob:
                        jobPidList = [pid,]
                        pidChildrenPidList = pidsDic[pid]['children_pids']
                        jobPidList.extend(pidChildrenPidList)

        return(jobPidList)

    def getJobPidListDic(self, pidsDic):
        print('>>> Getting openlava job related pids ...')

        bjobsUfDic = openlava_common.getBjobsUfInfo(command='bjobs -u all -r -m ' + str(hostname) + ' -UF')
        jobPidListDic = {}

        for job in bjobsUfDic.keys():
            if ('pids' in bjobsUfDic[job]) and (len(bjobsUfDic[job]['pids']) > 0):
                jobPidListDic[job] = [int(x) for x in bjobsUfDic[job]['pids']]
            else:
                jobPidList = self.getJobPidListDicFromProcessTree(pidsDic, job)
                if len(jobPidList) > 0:
                    jobPidListDic[job] = self.getJobPidListDicFromProcessTree(pidsDic, job)

        return(jobPidListDic)

    def getJobDic(self, jobPidListDic, pidProcessDic):
        print('>>> Getting openlava job related pid cpu/memory usage information ...')

        jobRelatedPids = []

        for job in jobPidListDic.keys():
            jobPids = jobPidListDic[job]
            jobRelatedPids.extend(jobPids)

        time.sleep(1)
        (jobRelatedPidDic, pidProcessDic) = self.getPidsDic(jobRelatedPids, pidProcessDic)
        jobResourceDic = {}

        for job in jobPidListDic.keys():
            jobResourceDic[job] = {}
            jobPids = jobPidListDic[job]

            for pid in jobPids:
                cpu = jobRelatedPidDic[pid]['cpu_percent']
                memory = jobRelatedPidDic[pid]['memory']

                if 'cpu' in jobResourceDic[job].keys():
                    jobResourceDic[job]['cpu'] = jobResourceDic[job]['cpu'] + cpu
                else:
                    jobResourceDic[job]['cpu'] = cpu

                if 'memory' in jobResourceDic[job].keys():
                    jobResourceDic[job]['memory'] = jobResourceDic[job]['memory'] + memory
                else:
                    jobResourceDic[job]['memory'] = memory

            jobResourceDic[job]['cpu'] = round(jobResourceDic[job]['cpu'], 4)
            jobResourceDic[job]['memory'] = round(jobResourceDic[job]['memory'], 4)

        return(jobResourceDic)

    def debugPrint(self, pidsDic, jobPidListDic):
        for job in jobPidListDic.keys():
            print('    JOB : ' + str(job))
            jobPids = jobPidListDic[job]
            print('        PIDS : ' + str(jobPids))
            for i in range(len(jobPids)):
                pid = jobPids[i]
                if pid in pidsDic.keys():
                    print('    '*(i+1) + '    PID (' + str(pid) + '): ' + str(pidsDic[pid]))
                else:
                    print('*Warning*: pid "' + str(pid) + '": informaiton missing.')

    def resultPrint(self, jobResourceDic):
        print('')
        print('JOB          CPU     MEMORY')
        print('===========================')
        for job in jobResourceDic.keys():
            cpu = jobResourceDic[job]['cpu']
            memory = jobResourceDic[job]['memory']
            print('%-9s    %4.2f    %4.2fG' % (job, cpu, memory))
        print('===========================')
        print('')

    def checkOldSqlTable(self, dbFile, orig_conn, tableName, keyList=['SAMPLE_TIME',]):
        dataDic = sqlite3_common.getSqlTableData(dbFile, orig_conn, tableName, keyList)

        if dataDic:
            if len(dataDic['SAMPLE_TIME']) > 0:
                lastSampleTime = dataDic['SAMPLE_TIME'][-1]
                lastSeconds = int(time.mktime(datetime.datetime.strptime(str(lastSampleTime), "%Y-%m-%d %H:%M:%S").timetuple()))
                if currentSeconds-lastSeconds > 3600:
                    common.printWarning('*Warning*: table "' + str(tableName) + '" already existed even one hour ago, will drop it.')
                    return(1)

        return(0)

    def saveJobDb(self, jobResourceDic):
        print('>>> Saving job related cpu/memory information into sqlite3 ...')
        common.debug('Saving job resource info into sqlite3 ...')

        jobList = list(jobResourceDic.keys())
        jobRangeDic = common.getJobRangeDic(jobList)

        keyList = ['SAMPLE_TIME', 'HOST_NAME', 'CPU', 'MEMORY']
        keyString = sqlite3_common.genSqlTableKeyString(keyList)

        for jobRange in jobRangeDic.keys():
            jobResourceSqlDic = {}
            dbFile = str(self.dbPath) +'/job/' + str(jobRange) + '.db'
            (result, dbConn) = sqlite3_common.connectDbFile(dbFile, mode='write')

            if result == 'passed':
                jobTableList = sqlite3_common.getSqlTableList(dbFile, dbConn)
            else:
                jobTableList = []
                print('*Error*: Failed on connecting sqlite3 database "' + str(dbFile) + '".')
                continue

            for job in jobRangeDic[jobRange]:
                jobResourceSqlDic[job] = {
                                  'drop': False,
                                  'keyString': '',
                                  'valueString': '',
                                 }
                tableName = job
                print('    Sampling for job "' + str(job) + '" ...')

                if tableName in jobTableList:
                    returnCode = self.checkOldSqlTable(dbFile, dbConn, tableName)
                    if returnCode == 1:
                        jobResourceSqlDic[job]['drop'] = True
                        jobResourceSqlDic[job]['keyString'] = keyString
                else:
                    jobResourceSqlDic[job]['keyString'] = keyString

                valueList = [sampleTime, hostname, jobResourceDic[job]['cpu'], jobResourceDic[job]['memory']]
                valueString = sqlite3_common.genSqlTableValueString(valueList)
                jobResourceSqlDic[job]['valueString'] = valueString

            for job in jobResourceSqlDic.keys():
                tableName = job
                if jobResourceSqlDic[job]['drop']:
                    print('    Dropping table "' + str(tableName) + '" ...')
                    sqlite3_common.dropSqlTable(dbFile, dbConn, tableName, commit=False)
                if jobResourceSqlDic[job]['keyString'] != '':
                    print('    Creating table "' + str(tableName) + '" ...')
                    sqlite3_common.createSqlTable(dbFile, dbConn, tableName, jobResourceSqlDic[job]['keyString'], commit=False)
                if jobResourceSqlDic[job]['valueString'] != '':
                    print('    Updating table "' + str(tableName) + '" with content "' + str(jobResourceSqlDic[job]['valueString']) + '" ...')
                    sqlite3_common.insertIntoSqlTable(dbFile, dbConn, tableName, jobResourceSqlDic[job]['valueString'], commit=False)

            if result == 'passed':
                dbConn.commit()
                dbConn.close()
                common.debug('Saving job resource info done.')

    def sample(self):
        # Get all pids on current host.
        pids = self.getAllNoRootPids()

        # Get pids informatins (pidsDic) and pid psutil Process (pidProcessDic).
        (pidsDic, pidProcessDic) = self.getPidsDic(pids)

        # Get job related pids (jobPidListDic, job: [pid1 pid2 ...]).
        jobPidListDic = self.getJobPidListDic(pidsDic)

        # Print debug information for pid/job.
        #self.debugPrint(pidsDic, jobPidListDic)

        # Re-collect job related pid informations (especially the cpu_percent information).
        (jobResourceDic) = self.getJobDic(jobPidListDic, pidProcessDic)

        # Show result.
        self.resultPrint(jobResourceDic)

        # Save database;
        self.saveJobDb(jobResourceDic)

#################
# Main Function #
#################
def main():
    myJobResourceSample = jobResourceSample()
    myJobResourceSample.sample()

if __name__ == '__main__':
    main()
