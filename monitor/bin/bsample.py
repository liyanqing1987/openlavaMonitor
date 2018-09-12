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
        (result, jobDbConn) = sqlite3_common.connectDbFile(jobDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling job info into ' + str(jobDbFile) + ' ...')

        jobTableList = sqlite3_common.getSqlTableList(jobDbFile, jobDbConn)
        bjobsDic = openlava_common.getBjobsUfInfo()
        jobList = list(bjobsDic.keys())
        jobSqlDic = {}

        for job in jobList:
            jobSqlDic[job] = {
                              'drop': False,
                              'keyString': '',
                              'valueString': '',
                             }
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
                        if self.currentSeconds-lastSeconds > 3600:
                            common.printWarning('    *Warning*: table "' + str(jobTableName) + '" already existed even one hour ago, will drop it.')
                            jobSqlDic[job]['drop'] = True
                            jobTableList.remove(jobTableName)

            # If job table is not on the jobDbFile, create it.
            if jobTableName not in jobTableList:
                keyList = self.addKeyDateInfo(keyList)
                keyString = sqlite3_common.genSqlTableKeyString(keyList)
                jobSqlDic[job]['keyString'] = keyString

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            jobSqlDic[job]['valueString'] = valueString

        for job in jobSqlDic.keys():
            jobTableName='job_' + str(job)
            if jobSqlDic[job]['drop']:
                sqlite3_common.dropSqlTable(jobDbFile, jobDbConn, jobTableName, commit=False)
            if jobSqlDic[job]['keyString'] != '':
                sqlite3_common.createSqlTable(jobDbFile, jobDbConn, jobTableName, jobSqlDic[job]['keyString'], commit=False)
            if jobSqlDic[job]['valueString'] != '':
                sqlite3_common.insertIntoSqlTable(jobDbFile, jobDbConn, jobTableName, jobSqlDic[job]['valueString'], commit=False)

        print('    Committing the update to sqlite3 ...')
        jobDbConn.commit()
        jobDbConn.close()
        print('    Done (' + str(len(jobList)) + ' jobs).')

    def sampleQueueInfo(self):
        """
        Sample queue info and save it into sqlite db.
        """
        self.getDateInfo()
        queueDbFile = str(self.dbPath) + '/queue.db'
        (result, queueDbConn) = sqlite3_common.connectDbFile(queueDbFile, mode='write')
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
        queueSqlDic = {}

        for i in range(len(queueList)):
            queue = queueList[i]
            queueSqlDic[queue] = {
                                  'keyString': '',
                                  'valueString': '',
                                 }
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
                queueSqlDic[queue]['keyString'] = keyString

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            queueSqlDic[queue]['valueString'] = valueString

        for queue in queueList:
            queueTableName = 'queue_' + str(queue)
            if queueSqlDic[queue]['keyString'] != '':
                sqlite3_common.createSqlTable(queueDbFile, queueDbConn, queueTableName, queueSqlDic[queue]['keyString'], commit=False)
            if queueSqlDic[queue]['valueString'] != '':
                sqlite3_common.insertIntoSqlTable(queueDbFile, queueDbConn, queueTableName, queueSqlDic[queue]['valueString'], commit=False)

        print('    Committing the update to sqlite3 ...')
        queueDbConn.commit()
        queueDbConn.close()

    def sampleHostInfo(self):
        """
        Sample host info and save it into sqlite db.
        """
        self.getDateInfo()
        hostDbFile = str(self.dbPath) + '/host.db'
        (result, hostDbConn) = sqlite3_common.connectDbFile(hostDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling host info into ' + str(hostDbFile) + ' ...')

        hostTableList = sqlite3_common.getSqlTableList(hostDbFile, hostDbConn)
        bhostsDic = openlava_common.getBhostsInfo()
        hostList = bhostsDic['HOST_NAME']

        # Insert 'sampleTime' into key list.
        origKeyList = list(bhostsDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)
        hostSqlDic = {}

        for i in range(len(hostList)):
            host = hostList[i]
            hostSqlDic[host] = {
                                'keyString': '',
                                'valueString': '',
                               }
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
                hostSqlDic[host]['keyString'] = keyString

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            hostSqlDic[host]['valueString'] = valueString

        for host in hostList:
            hostTableName = 'host_' + str(host)
            if hostSqlDic[host]['keyString'] != '':
                sqlite3_common.createSqlTable(hostDbFile, hostDbConn, hostTableName, hostSqlDic[host]['keyString'], commit=False)
            if hostSqlDic[host]['valueString'] != '':
                sqlite3_common.insertIntoSqlTable(hostDbFile, hostDbConn, hostTableName, hostSqlDic[host]['valueString'], commit=False)

        print('    Committing the update to sqlite3 ...')
        hostDbConn.commit()
        hostDbConn.close()

    def sampleLoadInfo(self):
        """
        Sample host load info and save it into sqlite db.
        """
        self.getDateInfo()
        loadDbFile = str(self.dbPath) + '/load.db'
        (result, loadDbConn) = sqlite3_common.connectDbFile(loadDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling host load info into ' + str(loadDbFile) + ' ...')

        loadTableList = sqlite3_common.getSqlTableList(loadDbFile, loadDbConn)
        lsloadDic = openlava_common.getLsloadInfo()
        hostList = lsloadDic['HOST_NAME']

        # Insert 'sampleTime' into key list.
        origKeyList = list(lsloadDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)
        loadSqlDic = {}

        for i in range(len(hostList)):
            host = hostList[i]
            loadSqlDic[host] = {
                                'keyString': '',
                                'valueString': '',
                               }
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
                loadSqlDic[host]['keyString'] = keyString

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            loadSqlDic[host]['valueString'] = valueString

        for host in hostList:
            loadTableName = 'load_' + str(host)
            if loadSqlDic[host]['keyString'] != '':
                sqlite3_common.createSqlTable(loadDbFile, loadDbConn, loadTableName, loadSqlDic[host]['keyString'], commit=False)
            if loadSqlDic[host]['valueString'] != '':
                sqlite3_common.insertIntoSqlTable(loadDbFile, loadDbConn, loadTableName, loadSqlDic[host]['valueString'], commit=False)

        print('    Committing the update to sqlite3 ...')
        loadDbConn.commit()
        loadDbConn.close()

    def sampleUserInfo(self):
        """
        Sample user info and save it into sqlite db.
        """
        self.getDateInfo()
        userDbFile = str(self.dbPath) + '/user.db'
        (result, userDbConn) = sqlite3_common.connectDbFile(userDbFile, mode='write')
        if result != 'passed':
            return

        print('>>> Sampling user info into ' + str(userDbFile) + ' ...')

        userTableList = sqlite3_common.getSqlTableList(userDbFile, userDbConn)
        busersDic = openlava_common.getBusersInfo()
        userList = busersDic['USER/GROUP']

        # Insert 'sampleTime' into key list.
        origKeyList = list(busersDic.keys())
        keyList = self.addKeyDateInfo(origKeyList)
        userSqlDic = {}

        for i in range(len(userList)):
            user = userList[i]
            userSqlDic[user] = {
                                'keyString': '',
                                'valueString': '',
                               }
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
                userSqlDic[user]['keyString'] = keyString

            # Insert sql table value.
            valueString = sqlite3_common.genSqlTableValueString(valueList)
            userSqlDic[user]['valueString'] = valueString

        for user in userList:
            userTableName = 'user_' + str(user)
            if userSqlDic[user]['keyString'] != '':
                sqlite3_common.createSqlTable(userDbFile, userDbConn, userTableName, userSqlDic[user]['keyString'], commit=False)
            if userSqlDic[user]['valueString'] != '':
                sqlite3_common.insertIntoSqlTable(userDbFile, userDbConn, userTableName, userSqlDic[user]['valueString'], commit=False)

        print('    Committing the update to sqlite3 ...')
        userDbConn.commit()
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

            p.join()

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
