#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import getpass
import datetime
import sqlite3

# Import openlavaMonitor packages.
from monitor.bin import bmonitorGUI
from monitor.conf import config
from monitor.common import common

os.environ["PYTHONUNBUFFERED"]="1"

user = getpass.getuser()

def drawJobMemCurve(job):
    """
    Draw memory usage curve for specified job.
    """
    print('>>> Drawing memory curve for job "' + str(job) + '" ...')

    runTimeList = []
    memList  = []

    dbFile= str(config.dbPath) + '/job.db'

    if not os.path.exists(dbFile):
        warningMessage = '*Warning*: No sampling date for job info.'
        common.printWarning(warningMessage)
        return

    conn = sqlite3.connect(dbFile)
    curs = conn.cursor()
    command = '''SELECT sampleTime,mem FROM {tableName}'''.format(tableName='job_' + str(job))
    results = curs.execute(command)
    allItems = results.fetchall()
    for item in allItems:
        (time, mem) = item
        runTimeList.append(time)
        memList.append(mem)
    curs.close()
    conn.commit()
    conn.close()

    if len(runTimeList) == 0:
        warningMessage = '*Warning*: No memory information for job "' + str(job) + '".'
        common.printWarning(warningMessage)
        return
    else:
        realRunTimeList = []
        realMemList = []
        firstRunTime = datetime.datetime.strptime(str(runTimeList[0]), '%Y%m%d_%H%M%S').timestamp()

        for i in range(len(runTimeList)):
            runTime = runTimeList[i]
            currentRunTime = datetime.datetime.strptime(str(runTime), '%Y%m%d_%H%M%S').timestamp()
            realRunTime = int((currentRunTime-firstRunTime)/60)
            realRunTimeList.append(realRunTime)
            mem = memList[i]
            if mem == '':
                 mem = '0'
            realMem = round(int(mem)/1024, 1)
            realMemList.append(realMem)

        memCurveFig = str(config.tempPath) + '/' + str(user) + '_' + str(job) + '.png'
        jobNum = common.stringToInt(job) 

        print('    Save memory curve as "' + str(memCurveFig) + '".')
        common.drawPlot(realRunTimeList, realMemList, 'runTime (Minitu)', 'memory (G)', yUnit='G', title='job : ' + str(job), saveName=memCurveFig, figureNum=jobNum)

def drawQueueJobNumCurve(queue):
    """
    Draw (PEND/RUN) job number curve for specified queue.
    """
    print('>>> Drawing queue (PEND/RUN) job num curve for queue "' + str(queue) + '" ...')

    dateList = []
    pendList = []
    runList = []
    tempPendList = []
    tempRunList = []

    dbFile= str(config.dbPath) + '/queue.db'
    if not os.path.exists(dbFile):
        warningMessage = '*Warning*: No sampling date for queue info.'
        common.printWarning(warningMessage)
        return

    conn = sqlite3.connect(dbFile)
    curs = conn.cursor()
    command = '''SELECT DATE,PEND,RUN FROM {tableName} WHERE QUEUE_NAME='{queueName}' '''.format(tableName='queue_' + str(queue), queueName=queue)
    results = curs.execute(command)
    allItems = results.fetchall()
    for i in range(len(allItems)):
        (date, pendNum, runNum) = allItems[i]

        if (i != 0) and ((i == len(allItems)-1) or (date not in dateList)):
            pendAvg = int(sum(tempPendList)/len(tempPendList))
            pendList.append(pendAvg)
            runAvg = int(sum(tempRunList)/len(tempRunList))
            runList.append(runAvg)

        if date not in dateList:
            dateList.append(date)
            tempPendList = []
            tempRunList = []

        tempPendList.append(int(pendNum))
        tempRunList.append(int(runNum))
    curs.close()
    conn.commit()
    conn.close()

    if len(dateList) == 0:
        warningMessage = '*Warning*: No (PEND/RUN) job number info for queue "' + str(queue) + '".'
        common.printWarning(warningMessage)
        return
    else:
        queueJobNumCurveFig = str(config.tempPath) + '/' + str(user) + '_' + str(queue) + '_jobNum.png'
        queueNum = common.stringToInt(queue) 

        print('    Save queue (PEND/RUN) job numeber curve as "' + str(queueJobNumCurveFig) + '".')
        common.drawPlots(dateList, [pendList, runList], 'DATE', 'NUM', ['PEND', 'RUN'], xIsString=True, title='queue : ' + str(queue), saveName=queueJobNumCurveFig, figureNum=queueNum)


#################
# Main Function #
#################
def main():
    bmonitorGUI.main()

if __name__ == '__main__':
    main()
