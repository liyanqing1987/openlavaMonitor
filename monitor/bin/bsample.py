#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import argparse
import sqlite3
import datetime

# Import openlavaMonitor packages.
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
                        help='Sample host info with command "bhosts/lshosts/lsload".')
    parser.add_argument("-u", "--user",
                        action="store_true", default=False,
                        help='Sample user info with command "busers".')
 
    args = parser.parse_args()
    return(args.job, args.queue, args.host, args.user)

class sampling:
    """
    Sample openlava basic information with openlava bjobs/bqueues/bhosts/lshosts/lsload/busers commands.
    Save the infomation into sqlite3 DB.
    """
    def __init__(self, jobSampling, queueSampling, hostSampling, userSampling):
        self.jobSampling = jobSampling
        self.queueSampling = queueSampling
        self.hostSampling = hostSampling
        self.userSampling = userSampling

        self.dbPath = config.dbPath 

    def sampleJobInfo(self):
        """
        Sample job info, especially the memory usage info.
        """
        print('>>> Sampling job info ...')
        currentDate = datetime.datetime.today().strftime('%Y%m%d')
        currentTime = datetime.datetime.today().strftime('%H%M%S')
        myDic = common.getBjobsUfInfo()
        jobList = list(myDic.keys())

        dbFile = str(self.dbPath) + '/job.db'
        print('    Infos will be saved into data base file "' + str(dbFile) + '".')
        conn = sqlite3.connect(dbFile)
        curs = conn.cursor()

        for job in jobList:
            print('    Saving for job "' + str(job) + '" ...')
            jobDic = myDic[job]
            # Only save "cpuTime" and "mem" infos.
            #keyList = list(jobDic.keys())
            keyList = ['cpuTime', 'mem']
            #valueList = list(jobDic.values())
            valueList = [jobDic['cpuTime'], jobDic['mem']]

            # Insert 'DATE' and 'TIME' into key list as two keys.
            # DATE/TIME is the sampling time.
            keyList.insert(0, 'sampleTime')
            keyList.insert(1, 'DATE')
            keyList.insert(2, 'TIME')
            valueList.insert(0, str(currentDate) + '_' + str(currentTime))
            valueList.insert(1, currentDate)
            valueList.insert(2, currentTime)
 
            # Generate sql (job) table title string.
            tableTitleString = '('
            for i in range(len(keyList)):
                key = keyList[i]
                if i == 0:
                    tableTitleString = str(tableTitleString) + "'" + str(key) + "' VARCHAR(255) PRIMARY KEY,"
                elif i == len(keyList)-1:
                    tableTitleString = str(tableTitleString) + " '" + str(key) + "' VARCHAR(255));"
                else:
                    tableTitleString = str(tableTitleString) + " '" + str(key) + "' VARCHAR(255),"

            # Generate sql table title.
            try:
                command = '''CREATE TABLE IF NOT EXISTS '{tableName}' {titleString} '''.format(tableName='job_' + str(job), titleString=tableTitleString)
                common.openlavaDebug(command)
                curs.execute(command)
            except Exception as error:
                common.printError('*Error*: Failed on creating table "job_' + str(job) + '" on db file "' + str(dbFile) + '": ' + str(error))
                sys.exit(1)
            
            # Generate sql (job) table value string.
            tableValueString = '('
            for i in range(len(valueList)):
                value = valueList[i]
                if i == 0:
                    tableValueString = str(tableValueString) + "'" + str(value) + "',"
                elif i == len(valueList)-1:
                    tableValueString = str(tableValueString) + " '" + str(value) + "');"
                else:
                    tableValueString = str(tableValueString) + " '" + str(value) + "',"

            # Insert sql table value.
            try:
                command = '''INSERT INTO '{tableName}' VALUES {valueString} '''.format(tableName='job_' + str(job), valueString=tableValueString)
                common.openlavaDebug(command)
                curs.execute(command) 
            except Exception as error:
                common.printError('*Error*: Failed on inserting value into table "job_' + str(job) + '" on db file "' + str(dbFile) + '": ' + str(error))
                sys.exit(1)

        curs.close()
        conn.commit()
        conn.close()
       
    def sampleQueueInfo(self):
        """
        Sample queue info and save it into sqlite db.
        """
        print('>>> Sampling queue info ...')
        currentDate = datetime.datetime.today().strftime('%Y%m%d')
        currentTime = datetime.datetime.today().strftime('%H%M%S')
        myDic = common.getBqueuesInfo()

        dbFile = str(self.dbPath) + '/queue.db'
        print('    Infos will be saved into data base file "' + str(dbFile) + '".')
        conn = sqlite3.connect(dbFile)
        curs = conn.cursor()

        # Generate sql (queue) table title string.
        keyList = list(myDic.keys())
        tableTitleString = "('sampleTime' VARCHAR(255) PRIMARY KEY, 'DATE' VARCHAR(255), 'TIME' VARCHAR(255),"
        for i in range(len(keyList)):
            key = keyList[i]
            if i == len(keyList)-1:
                tableTitleString = str(tableTitleString) + " '" + str(key) + "' VARCHAR(255));"
            else:
                tableTitleString = str(tableTitleString) + " '" + str(key) + "' VARCHAR(255),"

        queueList = myDic['QUEUE_NAME']

        for i in range(len(queueList)): 
            queue = queueList[i]
            print('    Saving for queue "' + str(queue) + '" ...')

            # Generate sql table title.
            try:
                command = '''CREATE TABLE IF NOT EXISTS '{tableName}' {titleString} '''.format(tableName='queue_' + str(queue), titleString=tableTitleString)
                common.openlavaDebug(command)
                curs.execute(command)
            except Exception as error:
                common.printError('*Error*: Failed on creating table "queue_' + str(queue) + '" on db file "' + str(dbFile) + '": ' + str(error))
                sys.exit(1)

            # Generate sql (queue) table value string.
            tableValueString = "('" + str(currentDate) + "_" + str(currentTime) + "', '" + str(currentDate) + "', '" + str(currentTime) + "',"
            for j in range(len(keyList)):
                key = keyList[j]
                value = myDic[key][i]
                if j == len(keyList)-1:
                    tableValueString = str(tableValueString) + " '" + str(value) + "');"
                else:
                    tableValueString = str(tableValueString) + " '" + str(value) + "',"

            # Insert sql table value.
            try:
                command = '''INSERT INTO '{tableName}' VALUES {valueString} '''.format(tableName='queue_' + str(queue), valueString=tableValueString)
                common.openlavaDebug(command)
                curs.execute(command) 
            except Exception as error:
                common.printError('*Error*: Failed on inserting value into table "queue_' + str(queue) + '" on db file "' + str(dbFile) + '": ' + str(error))
                sys.exit(1)

        curs.close()
        conn.commit()
        conn.close()

    def sampleHostInfo(self):
        pass

    def sampleUserInfo(self):
        pass
 
    def sampling(self):
        if self.jobSampling:
            self.sampleJobInfo()
        if self.queueSampling:
            self.sampleQueueInfo()
        if self.hostSampling:
            self.sampleHostInfo()
        if self.userSampling:
            self.sampleUserInfo()


#################
# Main Function #
#################
def main():
    (job, queue, host, user) = readArgs()
    mySampling = sampling(job, queue, host, user)
    mySampling.sampling()

if __name__ == '__main__':
    main()
