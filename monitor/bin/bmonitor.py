#!PYTHONPATH
# -*- coding: utf-8 -*-

import os
import sys
import getpass
import datetime
import sqlite3

# Import openlavaMonitor packages.
if ('openlavaMonitor_development_path' in os.environ) and os.path.exists(os.environ['openlavaMonitor_development_path']):
    sys.path.insert(0, os.environ['openlavaMonitor_development_path'])

from monitor.bin import bmonitorGUI
from monitor.conf import config
from monitor.common import common

os.environ["PYTHONUNBUFFERED"]="1"

user = getpass.getuser()

def drawJobMemCurve(job):
    """
    Draw memory usage curve for specified job.
    """
    print('Drawing memory curve for job "' + str(job) + '".')

    runTimeList = []
    memList  = []

    dbFile= str(config.dbPath) + '/job.db'

    if not os.path.exists(dbFile):
        warningMessage = '*Warning*: No sampling date for job info.'
        common.printWarning(warningMessage)
        return()

    jobTableList = common.getSqlTableList(dbFile)
    tableName = 'job_' + str(job)

    if tableName not in jobTableList:
        warningMessage = '*Warning*: No job information for job "' + str(job) + '".'
        common.printWarning(warningMessage)
        return()
    else:
        dataDic = common.getSqlData(dbFile, tableName, origKeyList=['sampleTime', 'mem'])
        runTimeList = dataDic['sampleTime']
        memList = dataDic['mem']

    if len(runTimeList) == 0:
        warningMessage = '*Warning*: No memory information for job "' + str(job) + '".'
        common.printWarning(warningMessage)
        return()
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

        print('Save memory curve as "' + str(memCurveFig) + '".')
        common.drawPlot(realRunTimeList, realMemList, 'runTime (Minitu)', 'memory (G)', yUnit='G', title='job : ' + str(job), saveName=memCurveFig, figureNum=jobNum)

def drawQueueJobNumCurve(queue):
    """
    Draw (PEND/RUN) job number curve for specified queue.
    """
    print('Drawing queue (PEND/RUN) job num curve for queue "' + str(queue) + '".')

    dateList = []
    pendList = []
    runList = []
    tempPendList = []
    tempRunList = []

    dbFile= str(config.dbPath) + '/queue.db'

    if not os.path.exists(dbFile):
        warningMessage = '*Warning*: No sampling date for queue info.'
        common.printWarning(warningMessage)
        return()

    queueTableList = common.getSqlTableList(dbFile)
    tableName = 'queue_' + str(queue)

    if tableName not in queueTableList:
        warningMessage = '*Warning*: No queue information for queue "' + str(queue) + '".'
        common.printWarning(warningMessage)
        return()
    else:
        dataDic = common.getSqlData(dbFile, tableName, origKeyList=['DATE', 'PEND', 'RUN'])
        origDateList = dataDic['DATE']
        origPendList = dataDic['PEND']
        origRunList = dataDic['RUN']

        for i in range(len(origDateList)):
            date = origDateList[i]
            pendNum = origPendList[i]
            runNum = origRunList[i]

            if (i != 0) and ((i == len(origDateList)-1) or (date not in dateList)):
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

    if len(dateList) == 0:
        warningMessage = '*Warning*: No (PEND/RUN) job number info for queue "' + str(queue) + '".'
        common.printWarning(warningMessage)
        return()
    else:
        queueJobNumCurveFig = str(config.tempPath) + '/' + str(user) + '_' + str(queue) + '_jobNum.png'
        queueNum = common.stringToInt(queue)

        print('Save queue (PEND/RUN) job numeber curve as "' + str(queueJobNumCurveFig) + '".')
        common.drawPlots(dateList, [pendList, runList], 'DATE', 'NUM', ['PEND', 'RUN'], xIsString=True, title='queue : ' + str(queue), saveName=queueJobNumCurveFig, figureNum=queueNum)

#################
# Main Function #
#################
def main():
    bmonitorGUI.main()

if __name__ == '__main__':
    main()
