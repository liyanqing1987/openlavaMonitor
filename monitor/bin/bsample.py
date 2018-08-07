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
from monitor.common import openlava_common
from monitor.common import sqlite3_common

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

    def getDateInfo(self):
        self.sampleTime = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
        self.currentSeconds = int(time.time())

    def addKeyDateInfo(self, inputKeyList):
        """
        Insert date info into key list.
        """
        keyList = copy.deepcopy(inputKeyList)
        keyList.insert(0, 'sampleTime')
        return(keyList)

    def addValueDateInfo(self, inputValueList):
        """
        Insert date info into value list.
        """
        valueList = copy.deepcopy(inputValueList)
        valueList.insert(0, str(self.sampleTime))
        return(valueList)

    def sampleJobInfo(self):
        """
        Sample job info, especially the memory usage info.
        """
        self.getDateInfo()
        jobDbFile = str(self.dbPath) + '/job.db'
        (result, jobDbConn, jobDbCurs) = sqlite3_common.connectDbFile(jobDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling job info into ' + str(jobDbFile) + ' ...')

        jobTableList = sqlite3_common.getSqlTableList(jobDbFile, jobDbConn)
        bjobsDic = openlava_common.getBjobsUfInfo()
        jobList = list(bjobsDic.keys())

        for job in jobList:
            jobTableName='job_' + str(job)
            print('    Sampling for job "' + str(job) + '" ...')

            # Insert 'sampleTime' into key list.
            jobDic = bjobsDic[job]
            keyList = list(jobDic.keys())
            keyList.pop()
            valueList = list(jobDic.values())
            valueList.pop()
            valueList = self.addValueDateInfo(valueList)

            # If job table (with old data) has been on the jobDbFile, drop it.
            if jobTableName in jobTableList:
                dataDic = sqlite3_common.getSqlTableData(jobDbFile, jobDbConn, jobTableName, ['sampleTime'])
                if dataDic:
                    if len(dataDic['sampleTime']) > 0:
                        lastSampleTime = dataDic['sampleTime'][-1]
                        lastSeconds = int(time.mktime(datetime.datetime.strptime(str(lastSampleTime), "%Y%m%d_%H%M%S").timetuple()))
                        if self.currentSeconds-lastSeconds > 864000:
                            common.printWarning('*Warning*: table "' + str(jobTableName) + '" already existed even ten day ago, will drop it.')
                            sqlite3_common.dropSqlTable(jobDbFile, jobDbConn, jobTableName)

            # If job table is not on the jobDbFile, create it.
            if jobTableName not in jobTableList:
                keyList = self.addKeyDateInfo(keyList)
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                sqlite3_common.createSqlTable(jobDbFile, jobDbConn, jobTableName, keyString)

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            sqlite3_common.insertIntoSqlTable(jobDbFile, jobDbConn, jobTableName, valueString)

        print('    Done')

        jobDbCurs.close()
        jobDbConn.close()

    def sampleQueueInfo(self):
        """
        Sample queue info and save it into sqlite db.
        """
        self.getDateInfo()
        queueDbFile = str(self.dbPath) + '/queue.db'
        (result, queueDbConn, queueDbCurs) = sqlite3_common.connectDbFile(queueDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling queue info into ' + str(queueDbFile) + ' ...')

        queueTableList = sqlite3_common.getSqlTableList(queueDbFile, queueDbConn)
        bqueuesDic = openlava_common.getBqueuesInfo()
        queueList = bqueuesDic['QUEUE_NAME']
        queueHostDic = openlava_common.getQueueHostInfo()

        # Insert 'sampleTime' into key list.
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
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                sqlite3_common.createSqlTable(queueDbFile, queueDbConn, queueTableName, keyString)

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            sqlite3_common.insertIntoSqlTable(queueDbFile, queueDbConn, queueTableName, valueString)

        queueDbCurs.close()
        queueDbConn.close()

    def sampleHostInfo(self):
        """
        Sample host info and save it into sqlite db.
        """
        self.getDateInfo()
        hostDbFile = str(self.dbPath) + '/host.db'
        (result, hostDbConn, hostDbCurs) = sqlite3_common.connectDbFile(hostDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling host info into ' + str(hostDbFile) + ' ...')

        hostTableList = sqlite3_common.getSqlTableList(hostDbFile, hostDbConn)
        bhostsDic = openlava_common.getBhostsInfo()
        hostList = bhostsDic['HOST_NAME']

        # Insert 'sampleTime' into key list.
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
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                sqlite3_common.createSqlTable(hostDbFile, hostDbConn, hostTableName, keyString)

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            sqlite3_common.insertIntoSqlTable(hostDbFile, hostDbConn, hostTableName, valueString)

        hostDbCurs.close()
        hostDbConn.close()

    def sampleLoadInfo(self):
        """
        Sample host load info and save it into sqlite db.
        """
        self.getDateInfo()
        loadDbFile = str(self.dbPath) + '/load.db'
        (result, loadDbConn, loadDbCurs) = sqlite3_common.connectDbFile(loadDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling host load info into ' + str(loadDbFile) + ' ...')

        loadTableList = sqlite3_common.getSqlTableList(loadDbFile, loadDbConn)
        lsloadDic = openlava_common.getLsloadInfo()
        hostList = lsloadDic['HOST_NAME']

        # Insert 'sampleTime' into key list.
        origKeyList = list(lsloadDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)

        for i in range(len(hostList)):
            host = hostList[i]
            loadTableName = 'load_' + str(host)
            print('    Sampling for host "' + str(host) + '" ...')

            # Get the host load value infos.
            valueList = []
            valueList = self.addValueDateInfo(valueList)
            for key in origKeyList:
                keyValue = lsloadDic[key][i]
                valueList.append(keyValue)

            # Generate sql table.
            if loadTableName not in loadTableList:
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                sqlite3_common.createSqlTable(loadDbFile, loadDbConn, loadTableName, keyString)

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            sqlite3_common.insertIntoSqlTable(loadDbFile, loadDbConn, loadTableName, valueString)

        loadDbCurs.close()
        loadDbConn.close()

    def sampleUserInfo(self):
        """
        Sample user info and save it into sqlite db.
        """
        self.getDateInfo()
        userDbFile = str(self.dbPath) + '/user.db'
        (result, userDbConn, userDbCurs) = sqlite3_common.connectDbFile(userDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling user info into ' + str(userDbFile) + ' ...')

        userTableList = sqlite3_common.getSqlTableList(userDbFile, userDbConn)
        busersDic = openlava_common.getBusersInfo()
        userList = busersDic['USER/GROUP']

        # Insert 'sampleTime' into key list.
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
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                sqlite3_common.createSqlTable(userDbFile, userDbConn, userTableName, keyString)

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            sqlite3_common.insertIntoSqlTable(userDbFile, userDbConn, userTableName, valueString)

        userDbCurs.close()
        userDbConn.close()

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
