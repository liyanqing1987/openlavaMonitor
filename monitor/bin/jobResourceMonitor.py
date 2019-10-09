#!PYTHONPATH
# -*- coding: utf-8 -*-
#
# NOTICE:
# 1. Make sure the script execution account have permission to access <dbPath>/resource.
# 2. Execute this script on crontab (Interval : 5 minutes).

import os
import sys
import time
import datetime
import argparse

sys.path.append('MONITORPATH')
from common import common
from common import openlava_common
from common import sqlite3_common
from conf import config

os.environ["PYTHONUNBUFFERED"] = "1"

def readArgs():
    """
    Read in arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-j", "--jobs",
                        nargs='+', default=[],
                        help='Specify job(s) to monitor, debug mode.')

    args = parser.parse_args()
    return(args.jobs)

class jobResourceMonitor:
    """
    If the job is 'RUN' on last sampling and "DONE/EXIT" on latest sampling, monitor it's memory usage, send mail if any issue.
    """
    def __init__(self, jobs):
        self.specifiedJobs = jobs
        self.finishedJobDic = {}
        self.finishedJobList = []

        self.dbPath = str(config.dbPath) + '/resource'
        userDbPath = str(self.dbPath) + '/user'

        if not os.path.exists(userDbPath):
            try:
                print('mkdir -p ' + str(userDbPath))
                os.system('mkdir -p ' + str(userDbPath))
            except:
                print('*Error*: Failed on creating sqlite user db directory "' + str(userDbPath) + '".')
                sys.exit(1)

    def getLastJobList(self):
        lastJobList = []
        lastJobListFile = str(self.dbPath) + '/job.list'

        if os.path.exists(lastJobListFile):
            with open(lastJobListFile, 'r') as LJLF:
                for line in LJLF:
                    lastJobList.append(line.strip())

        return(lastJobList)

    def getLatestJobList(self):
        """
        Sample job info with command 'bjobs -u all -a -UF'.
        """
        currentTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('[' + str(currentTime) + ']  Sampling job info ...')

        if len(self.specifiedJobs) == 0:
            latestJobDic = openlava_common.getBjobsInfo()
        else:
            jobsString = ' '.join(self.specifiedJobs)
            command='bjobs -w ' + str(jobsString)
            latestJobDic = openlava_common.getBjobsInfo(command)

        latestJobList = latestJobDic['JOBID']
        latestJobListFile = str(self.dbPath) + '/job.list'

        with open(latestJobListFile, 'w') as LJLF:
            for job in latestJobList:
                LJLF.write(str(job) + '\n')

        return(latestJobList)

    def getFinishedJobs(self):
        """
        Compare last and latest job list, pick up the new DONE/EXIT jobs on latest sampling.
        """
        if len(self.specifiedJobs) > 0:
            self.finishedJobList = self.specifiedJobs
        else:
            self.finishedJobList = []

            lastJobList = self.getLastJobList()
            latestJobList = self.getLatestJobList()

            self.finishedJobList = [job for job in lastJobList if job not in latestJobList]

        finishedJobString = ' '.join(self.finishedJobList)
        self.finishedJobDic = openlava_common.getBjobsUfInfo('bjobs -UF ' + str(finishedJobString))

        if len(self.specifiedJobs) == 0:
            for job in self.finishedJobList:
                if job in self.finishedJobDic:
                    jobStatus = self.finishedJobDic[job]['status']
                    if (jobStatus != 'DONE') and (jobStatus != 'EXIT'):
                        self.finishedJobList.remove(job)

    def connectUserDb(self, user):
        dbFile= str(self.dbPath) + '/user/' + str(user) + '.db'
        (result, dbConn) = sqlite3_common.connectDbFile(dbFile, mode='write')

        if result != 'passed':
            tableList = []
            print('*Error*: Failed on connecting sqlite3 database "' + str(dbFile) + '".')
        else:
            tableList = sqlite3_common.getSqlTableList(dbFile, dbConn)

        return(result, dbFile, dbConn, tableList)

    def writeUserData(self, job, jobRequestedProcessorsString, jobAvgCpuString, jobPeakCpuString, jobRusageMemString, jobAvgMemString, jobPeakMemString, jobRunTime):
        jobUser = self.finishedJobDic[job]['user']
        jobStatus = self.finishedJobDic[job]['status']
        jobCwd = self.finishedJobDic[job]['cwd']
        jobCommand = self.finishedJobDic[job]['command']
        tableName = jobUser

        keyList = ['SAMPLE_TIME', 'JOB', 'STATUS', 'CPU_RESERVED', 'CPU_AVG', 'CPU_PEAK', 'MEM_RESERVED', 'MEM_AVG', 'MEM_PEAK', 'RUN_TIME', 'CWD', 'COMMAND']
        keyTypeList = ['VARCHAR(20)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(10)', 'VARCHAR(1000)', 'VARCHAR(1000)']
        keyString = sqlite3_common.genSqlTableKeyString(keyList, keyTypeList, autoIncrement=True)

        (result, dbFile, dbConn, tableList) = self.connectUserDb(jobUser)

        if result == 'passed':
            if tableName not in tableList:
                sqlite3_common.createSqlTable(dbFile, dbConn, tableName, keyString)

            valueList = [self.sampleTime, job, jobStatus, jobRequestedProcessorsString, jobAvgCpuString, jobPeakCpuString, jobRusageMemString, jobAvgMemString, jobPeakMemString, jobRunTime, jobCwd, jobCommand]
            valueString = sqlite3_common.genSqlTableValueString(valueList, autoIncrement=True)
            sqlite3_common.insertIntoSqlTable(dbFile, dbConn, tableName, valueString)

    def sampleTimeCheck(self, job, sampleTimeList):
        """
        lastSampleTime - currentTime > 1 hour, fail.
        nextSampleTime - lastSampleTime > 1 hour, fail.
        """
        if len(self.specifiedJobs) > 0:
            return(0)
        else:
            if len(sampleTimeList) == 0:
                # No sampleTime data, fail.
                print('    * For job "' + str(job) + '", SAMPLE_TIME info is empty on sqlite3.')
                return(1)
            else:
                # lastSampleTime is too long time ago, fail.
                lastSampleTime = sampleTimeList[-1]
                lastSeconds = int(time.mktime(datetime.datetime.strptime(str(lastSampleTime), "%Y-%m-%d %H:%M:%S").timetuple()))

                if self.currentSeconds-lastSeconds > 3600:
                    print('    * For job "' + str(job) + '", last sampleTime info is on long time ago.')
                    return(1)

                # Data sample have ever been broken, fail.
                lastSeconds = ''

                for sampleTime in sampleTimeList:
                    seconds = int(time.mktime(datetime.datetime.strptime(str(sampleTime), "%Y-%m-%d %H:%M:%S").timetuple()))
                    if lastSeconds == '':
                        lastSeconds = seconds
                    else:
                        if seconds-lastSeconds > 3600:
                            print('    * For job "' + str(job) + '", sampleTime info have ever been broken.')
                            return(1)
                        else:
                            lastSeconds = seconds

                return(0)

    def getMultiHostPeakValue(self, sampleTimeList, hostList, valueList):
        # Update sampleTimeList to secondsList.
        secondsList = []

        for sampleTime in sampleTimeList:
            seconds = int(time.mktime(datetime.datetime.strptime(str(sampleTime), "%Y-%m-%d %H:%M:%S").timetuple()))
            secondsList.append(seconds)

        # Mark near by seconds.
        usedNum = []
        nearBySecondsList = []

        for i in range(len(secondsList)):
            if i in usedNum:
                continue
            else:
                usedNum.append(i)
                tmpNumList = [i]
                host = hostList[i]
                tmpHostList = [host]
                seconds = secondsList[i]
                for j in range(i+1, len(secondsList)):
                    newSeconds = secondsList[j]
                    if -5 <= newSeconds-seconds <= 5:
                        host = hostList[j]
                        if host not in tmpHostList:
                            usedNum.append(j)
                            tmpNumList.append(j)
                nearBySecondsList.append(tmpNumList)

        # Get collected values into sumValueList.
        sumValueList = []

        for nearBySeconds in nearBySecondsList:
            sumValue = 0
            for num in nearBySeconds:
                value = valueList[num]
                sumValue += value
            sumValueList.append(sumValue)

        peakValue = max(sumValueList)
        return(peakValue)

    def connectJobDb(self, job):
        jobRangeDic = common.getJobRangeDic([job,])
        jobRangeList = list(jobRangeDic.keys())
        jobRange = jobRangeList[0]

        dbFile= str(self.dbPath) + '/job/' + str(jobRange) + '.db'
        (result, dbConn) = sqlite3_common.connectDbFile(dbFile, mode='read')

        if result != 'passed':
            tableList = []
            print('*Error*: Failed on connecting sqlite3 database "' + str(dbFile) + '".')
        else:
            tableList = sqlite3_common.getSqlTableList(dbFile, dbConn)

        return(result, dbFile, dbConn, tableList)

    def getJobPeakAvg(self, job):
        """
        Get Job peak cpu info from job resource database file.
        """
        jobRunTime = 0
        jobPeakCpu = ''
        jobAvgCpu = ''
        jobPeakMem = ''
        jobAvgMem = ''

        tableName = job
        (result, dbFile, dbConn, tableList) = self.connectJobDb(job)

        if result == 'passed':
            if tableName not in tableList:
                print('    * For job "' + str(job) + '", job table "' + str(tableName) + '" is missing on sqlite3.')
            else:
                dataDic = sqlite3_common.getSqlTableData(dbFile, dbConn, tableName, keyList=['SAMPLE_TIME', 'HOST_NAME', 'CPU', 'MEMORY'])
                if 'SAMPLE_TIME' not in dataDic.keys():
                    print('    * For job "' + str(job) + '", SAMPLE_TIME info is missing on sqlite3 (table empty?).')
                else:
                    sampleTimeList = dataDic['SAMPLE_TIME']
                    returnCode = self.sampleTimeCheck(job, sampleTimeList)
                    if returnCode == 0:
                        jobRunTime = self.getJobRunTime(job)
                        cpuValueList = dataDic['CPU']
                        memValueList = dataDic['MEMORY']
                        cpuValueFloatList = [float(i) for i in cpuValueList]
                        memValueFloatList = [float(i) for i in memValueList]
                        jobAvgCpu = sum(cpuValueFloatList)/len(cpuValueFloatList)
                        jobAvgMem = sum(memValueFloatList)/len(memValueFloatList)
                        origHostList = dataDic['HOST_NAME']
                        hostList = list(set(origHostList))
                        if len(hostList) > 1:
                            print('    * Notice: job "' + str(job) + '" use multi-hosts.')
                            jobPeakCpu = self.getMultiHostPeakValue(sampleTimeList, origHostList, cpuValueFloatList)
                            jobPeakMem = self.getMultiHostPeakValue(sampleTimeList, origHostList, memValueFloatList)
                        else:
                            jobPeakCpu = max(cpuValueFloatList)
                            jobPeakMem = max(memValueFloatList)

        return(jobRunTime, jobPeakCpu, jobAvgCpu, jobPeakMem, jobAvgMem)

    def getJobRunTime(self, job):
        jobRunTime = 0

        if job in self.finishedJobDic.keys():
            jobStartedTime = self.finishedJobDic[job]['startedTime']
            jobFinishedTime = self.finishedJobDic[job]['finishedTime']
            if (jobStartedTime != '') and (jobFinishedTime != ''):
                jobStartedSeconds = int(time.mktime(datetime.datetime.strptime(str(jobStartedTime), "%a %b %d %H:%M:%S").timetuple()))
                jobFinishedSeconds = int(time.mktime(datetime.datetime.strptime(str(jobFinishedTime), "%a %b %d %H:%M:%S").timetuple()))
                jobRunTime = jobFinishedSeconds-jobStartedSeconds

                if jobRunTime < 0:
                    jobRunTime += 31536000

        return(jobRunTime)

    def monitorResourceUsage(self):
        """
        Monitor job cpu/memory usage.
        """
        print('    Resource usage info:')
        print('    =========')
        print('    %-10s%-16s%-10s%-13s%-14s%-14s%-13s%-14s%-14s%-14s'%('JOB', 'USER', 'STATUS', 'CPU_RESERVED', 'CPU_USED(avg)', 'CPU_USED(peak)', 'MEM_RESERVED', 'MEM_USED(avg)', 'MEM_USED(peak)', 'RUN_TIME'))

        for job in self.finishedJobList:
            if job in self.finishedJobDic:
                jobUser = self.finishedJobDic[job]['user']
                jobStatus = self.finishedJobDic[job]['status']
                jobRequestedProcessors = self.finishedJobDic[job]['processorsRequested']
                jobRusageMem = self.finishedJobDic[job]['rusageMem']

                (jobRunTime, jobPeakCpu, jobAvgCpu, jobPeakMem, jobAvgMem) = self.getJobPeakAvg(job)

                # For cpu
                if jobRequestedProcessors != '':
                    jobRequestedProcessorsString = str(jobRequestedProcessors)
                else:
                    jobRequestedProcessorsString = 'NA'

                if str(jobAvgCpu) != '':
                    jobAvgCpuString = str(round(float(jobAvgCpu), 1))
                else:
                    jobAvgCpuString = 'NA'

                if str(jobPeakCpu) != '':
                    jobPeakCpuString = str(round(float(jobPeakCpu), 1))
                else:
                    jobPeakCpuString = 'NA'

                # For mem
                if str(jobRusageMem) != '':
                    jobRusageMemString = str(round(float(jobRusageMem)/1024, 3)) + 'G'
                else:
                    jobRusageMemString = 'NA'

                if str(jobAvgMem) != '':
                    jobAvgMemString = str(round(float(jobAvgMem), 3)) + 'G'
                else:
                    jobAvgMemString = 'NA'

                if str(jobPeakMem) != '':
                    jobPeakMemString = str(round(float(jobPeakMem), 3)) + 'G'
                else:
                    jobPeakMemString = 'NA'

                print('    %-10s%-16s%-10s%-13s%-14s%-14s%-13s%-14s%-14s%-14s'%(job, jobUser, jobStatus, jobRequestedProcessorsString, jobAvgCpuString, jobPeakCpuString, jobRusageMemString, jobAvgMemString, jobPeakMemString, jobRunTime))

                if len(self.specifiedJobs) == 0:
                    self.writeUserData(job, jobRequestedProcessorsString, jobAvgCpuString, jobPeakCpuString, jobRusageMemString, jobAvgMemString, jobPeakMemString, jobRunTime)

        print('    =========\n')

    def monitor(self):
        """
        Get finished jobs.
        Monitor the cpu/memory usage, send warning email if any reserve-use mismatch.
        """
        self.sampleTime = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.currentSeconds = int(time.time())

        self.getFinishedJobs()

        if len(self.finishedJobList) == 0:
            print('    There is no finished job between two samples.')
            return
        else:
            finishedJobString = ' '.join(self.finishedJobList)
            print('    Finished jobs : ' + str(finishedJobString))

            self.monitorResourceUsage()

################
# Main Process #
################
def main():
    jobList = readArgs()
    myJobResourceMonitor = jobResourceMonitor(jobList)
    myJobResourceMonitor.monitor()

if __name__ == '__main__':
    main()
