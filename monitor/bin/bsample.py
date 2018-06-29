#!PYTHONPATH
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import datetime
import time
import copy
from multiprocessing import Process

# Import openlavaMonitor packages.
if ('openlavaMonitor_development_path' in os.environ) and os.path.exists(os.environ['openlavaMonitor_development_path']):
    sys.path.insert(0, os.environ['openlavaMonitor_development_path'])

from monitor.conf import config
from monitor.common import common

os.environ["PYTHONUNBUFFERED"]="1"

def readArgs():
    """
    Read arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-j", "--job",
                        action="store_true", default=False,
                        help='Sample running job info with command "bjobs -u all -r -UF".')
    parser.add_argument("-q", "--queue",
                        action="store_true", default=False,
                        help='Sample queue info with command "bqueues".')
    parser.add_argument("-H", "--host",
                        action="store_true", default=False,
                        help='Sample host info with command "bhosts".')
    parser.add_argument("-l", "--load",
                        action="store_true", default=False,
                        help='Sample host load info with command "lsload".')
    parser.add_argument("-u", "--user",
                        action="store_true", default=False,
                        help='Sample user info with command "busers".')
    parser.add_argument("-i", "--interval",
                        type=int,
                        default=0,
                        help='Specify the sampling interval, unit is second. Sampling only once by default".')

    args = parser.parse_args()

    if args.interval < 0:
        common.printError('*Error*: interval "' + str(args.interval) + '": Cannot be less than "0".')
        sys.exit(1)

    return(args.job, args.queue, args.host, args.load, args.user, args.interval)

class sampling:
    """
    Sample openlava basic information with openlava bjobs/bqueues/bhosts/lshosts/lsload/busers commands.
    Save the infomation into sqlite3 DB.
    """
    def __init__(self, jobSampling, queueSampling, hostSampling, loadSampling, userSampling, interval):
        self.jobSampling = jobSampling
        self.queueSampling = queueSampling
        self.hostSampling = hostSampling
        self.loadSampling = loadSampling
        self.userSampling = userSampling
        self.interval = interval

        self.dbPath = config.dbPath
        self.jobDbFile = str(self.dbPath) + '/job.db'
        self.queueDbFile = str(self.dbPath) + '/queue.db'
        self.hostDbFile = str(self.dbPath) + '/host.db'
        self.loadDbFile = str(self.dbPath) + '/load.db'
        self.userDbFile = str(self.dbPath) + '/user.db'

    def getDateInfo(self):
        self.currentDate = datetime.datetime.today().strftime('%Y%m%d')
        self.currentTime = datetime.datetime.today().strftime('%H%M%S')
        self.currentSeconds = int(time.time())

    def addKeyDateInfo(self, inputKeyList):
        """
        Insert date info into key list.
        """
        keyList = copy.deepcopy(inputKeyList)
        keyList.insert(0, 'sampleTime')
        keyList.insert(1, 'DATE')
        keyList.insert(2, 'TIME')
        keyList.insert(3, 'SECONDS')
        return(keyList)

    def addValueDateInfo(self, inputValueList):
        """
        Insert date info into value list.
        """
        valueList = copy.deepcopy(inputValueList)
        valueList.insert(0, str(self.currentDate) + '_' + str(self.currentTime))
        valueList.insert(1, self.currentDate)
        valueList.insert(2, self.currentTime)
        valueList.insert(3, self.currentSeconds)
        return(valueList)

    def sampleJobInfo(self):
        """
        Sample job info, especially the memory usage info.
        """
        self.getDateInfo()

        print('>>> Sampling job info into ' + str(self.jobDbFile) + ' ...')

        jobTableList = common.getSqlTableList(self.jobDbFile)
        bjobsDic = common.getBjobsUfInfo()
        jobList = list(bjobsDic.keys())

        for job in jobList:
            jobTableName='job_' + str(job)
            print('    Sampling for job "' + str(job) + '" ...')

            # Insert 'sampleTime', 'DATE', 'TIME' and 'SECONDS' into key list.
            jobDic = bjobsDic[job]
            keyList = list(jobDic.keys())
            valueList = list(jobDic.values())
            valueList = self.addValueDateInfo(valueList)

            # If job table (with old data) has been on the self.jobDbFile, drop it.
            if jobTableName in jobTableList:
                dataDic = common.getSqlData(self.jobDbFile, jobTableName, ['SECONDS'])
                lastSeconds = int(dataDic['SECONDS'][-1])
                if self.currentSeconds-lastSeconds > 86400:
                    common.printWarning('*Warning*: table "' + str(jobTableName) + '" already existed even one day ago, will drop it.')
                    common.dropSqlTable(self.jobDbFile, jobTableName)

            # If job table is not on the self.jobDbFile, create it.
            if jobTableName not in jobTableList:
                keyList = self.addKeyDateInfo(keyList)
                keyString = common.genSqlTableKeyString(keyList)
                common.createSqlTable(self.jobDbFile, jobTableName, keyString)

            # Insert sql table value.
            valueString = common.genSqlTableValueString(valueList)
            common.insertIntoSqlTable(self.jobDbFile, jobTableName, valueString)

    def sampleQueueInfo(self):
        """
        Sample queue info and save it into sqlite db.
        """
        self.getDateInfo()

        print('>>> Sampling queue info into ' + str(self.queueDbFile) + ' ...')

        queueTableList = common.getSqlTableList(self.queueDbFile)
        bqueuesDic = common.getBqueuesInfo()
        queueList = bqueuesDic['QUEUE_NAME']
        queueHostDic = common.getQueueHostInfo()

        # Insert 'sampleTime', 'DATE', 'TIME' and 'SECONDS' into key list.
        origKeyList = list(bqueuesDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)
        keyList.append('HOST')

        for i in range(len(queueList)):
            queue = queueList[i]
            queueTableName = 'queue_' + str(queue)
            print('    Sampling for queue "' + str(queue) + '" ...')

            # Get the queue value infos.
            valueList = []
            valueList = self.addValueDateInfo(valueList)
            for key in origKeyList:
                keyValue = bqueuesDic[key][i]
                valueList.append(keyValue)

            # Add queue-host info into queue value infos.
            queueHosts = queueHostDic[queue]
            hostString = ' '.join(queueHosts)
            valueList.append(hostString)

            # Generate sql table.
            if queueTableName not in queueTableList:
                keyString = common.genSqlTableKeyString(keyList)
                common.createSqlTable(self.queueDbFile, queueTableName, keyString)

            # Insert sql table value.
            valueString = common.genSqlTableValueString(valueList)
            common.insertIntoSqlTable(self.queueDbFile, queueTableName, valueString)

    def sampleHostInfo(self):
        """
        Sample host info and save it into sqlite db.
        """
        self.getDateInfo()

        print('>>> Sampling host info into ' + str(self.hostDbFile) + ' ...')

        hostTableList = common.getSqlTableList(self.hostDbFile)
        bhostsDic = common.getBhostsInfo()
        hostList = bhostsDic['HOST_NAME']

        # Insert 'sampleTime', 'DATE', 'TIME' and 'SECONDS' into key list.
        origKeyList = list(bhostsDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)

        for i in range(len(hostList)):
            host = hostList[i]
            hostTableName = 'host_' + str(host)
            print('    Sampling for host "' + str(host) + '" ...')

            # Get the host value infos.
            valueList = []
            valueList = self.addValueDateInfo(valueList)
            for key in origKeyList:
                keyValue = bhostsDic[key][i]
                valueList.append(keyValue)

            # Generate sql table.
            if hostTableName not in hostTableList:
                keyString = common.genSqlTableKeyString(keyList)
                common.createSqlTable(self.hostDbFile, hostTableName, keyString)

            # Insert sql table value.
            valueString = common.genSqlTableValueString(valueList)
            common.insertIntoSqlTable(self.hostDbFile, hostTableName, valueString)

    def sampleLoadInfo(self):
        """
        Sample host load info and save it into sqlite db.
        """
        self.getDateInfo()

        print('>>> Sampling host load info into ' + str(self.loadDbFile) + ' ...')

        loadTableList = common.getSqlTableList(self.loadDbFile)
        lsloadDic = common.getLsloadInfo()
        hostList = lsloadDic['HOST_NAME']

        # Insert 'sampleTime', 'DATE', 'TIME' and 'SECONDS' into key list.
        origKeyList = list(lsloadDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)

        for i in range(len(hostList)):
            host = hostList[i]
            loadTableName = 'host_' + str(host)
            print('    Sampling for host "' + str(host) + '" ...')

            # Get the host load value infos.
            valueList = []
            valueList = self.addValueDateInfo(valueList)
            for key in origKeyList:
                keyValue = lsloadDic[key][i]
                valueList.append(keyValue)

            # Generate sql table.
            if loadTableName not in loadTableList:
                keyString = common.genSqlTableKeyString(keyList)
                common.createSqlTable(self.loadDbFile, loadTableName, keyString)

            # Insert sql table value.
            valueString = common.genSqlTableValueString(valueList)
            common.insertIntoSqlTable(self.loadDbFile, loadTableName, valueString)

    def sampleUserInfo(self):
        """
        Sample user info and save it into sqlite db.
        """
        self.getDateInfo()

        print('>>> Sampling user info into ' + str(self.userDbFile) + ' ...')

        userTableList = common.getSqlTableList(self.userDbFile)
        busersDic = common.getBusersInfo()
        userList = busersDic['USER/GROUP']

        # Insert 'sampleTime', 'DATE', 'TIME' and 'SECONDS' into key list.
        origKeyList = list(busersDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)

        for i in range(len(userList)):
            user = userList[i]
            userTableName = 'user_' + str(user)
            print('    Sampling for user "' + str(user) + '" ...')

            # Get the user value infos.
            valueList = []
            valueList = self.addValueDateInfo(valueList)
            for key in origKeyList:
                keyValue = busersDic[key][i]
                valueList.append(keyValue)

            # Generate sql table.
            if userTableName not in userTableList:
                keyString = common.genSqlTableKeyString(keyList)
                common.createSqlTable(self.userDbFile, userTableName, keyString)

            # Insert sql table value.
            valueString = common.genSqlTableValueString(valueList)
            common.insertIntoSqlTable(self.userDbFile, userTableName, valueString)

    def sampling(self):
        while True:
            if self.jobSampling:
                p = Process(target=self.sampleJobInfo)
                p.start()
            if self.queueSampling:
                p = Process(target=self.sampleQueueInfo)
                p.start()
            if self.hostSampling:
                p = Process(target=self.sampleHostInfo)
                p.start()
            if self.loadSampling:
                p = Process(target=self.sampleLoadInfo)
                p.start()
            if self.userSampling:
                p = Process(target=self.sampleUserInfo)
                p.start()

            if self.interval == 0:
                 break
            elif self.interval > 0:
                 time.sleep(self.interval)

#################
# Main Function #
#################
def main():
    (job, queue, host, load, user, interval) = readArgs()
    mySampling = sampling(job, queue, host, load, user, interval)
    mySampling.sampling()

if __name__ == '__main__':
    main()
