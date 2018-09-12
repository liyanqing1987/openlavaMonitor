#!PYTHONPATH
# -*- coding: utf-8 -*-

import os
import re
import sys
import getpass
import datetime

# Import openlavaMonitor packages.
if ('openlavaMonitor_development_path' in os.environ) and os.path.exists(os.environ['openlavaMonitor_development_path']):
    sys.path.insert(0, os.environ['openlavaMonitor_development_path'])

from monitor.bin import bmonitorGUI
from monitor.conf import config
from monitor.common import common
from monitor.common import sqlite3_common

os.environ["PYTHONUNBUFFERED"]="1"

class drawCurve():
    def __init__(self):
        self.user = getpass.getuser()
        self.jobDbFile= str(config.dbPath) + '/job.db'
        self.queueDbFile= str(config.dbPath) + '/queue.db'

        (self.jobDbFileConnectResult, self.jobDbConn) = sqlite3_common.connectDbFile(self.jobDbFile)
        if self.jobDbFileConnectResult == 'failed':
            common.printWarning('*Warning*: Failed on connectiong job database file "' + str(self.jobDbFile) + '".')

        (self.queueDbFileConnectResult, self.queueDbConn) = sqlite3_common.connectDbFile(self.queueDbFile)
        if self.queueDbFileConnectResult == 'failed':
            common.printWarning('*Warning*: Failed on connectiong queue database file "' + str(self.queueDbFile) + '".')

        self.jobFirstLoad = True
        self.queueFirstLoad = True

    def __clear__(self):
        if self.jobDbFileConnectResult == 'passed':
            self.jobDbConn.close()

        if self.queueDbFileConnectResult == 'passed':
            self.queueDbConn.close()

    def drawJobMemCurve(self, job):
        """
        Draw memory usage curve for specified job.
        """
        print('Drawing memory curve for job "' + str(job) + '".')
    
        if self.jobDbFileConnectResult == 'failed':
            common.printWarning('*Warning*: Failed on connectiong job database file "' + str(self.jobDbFile) + '".')
            return
    
        runTimeList = []
        memList  = []

        if self.jobFirstLoad:
            common.printWarning('*Warning*: It is the first time loading job database, it may cost a little time ...')
            self.jobFirstLoad = False

        tableName = 'job_' + str(job)
        dataDic = sqlite3_common.getSqlTableData(self.jobDbFile, self.jobDbConn, tableName, ['sampleTime', 'mem'])

        if not dataDic:
            common.printWarning('*Warning*: job information is missing for "' + str(job) + '".')
            return
        else:
            runTimeList = dataDic['sampleTime']
            memList = dataDic['mem']
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

            memCurveFig = str(config.tempPath) + '/' + str(self.user) + '_' + str(job) + '.png'
            jobNum = common.stringToInt(job)

            print('Save memory curve as "' + str(memCurveFig) + '".')
            common.drawPlot(realRunTimeList, realMemList, 'runTime (Minitu)', 'memory (G)', yUnit='G', title='job : ' + str(job), saveName=memCurveFig, figureNum=jobNum)

    def drawQueueJobNumCurve(self, queue):
        """
        Draw (PEND/RUN) job number curve for specified queue.
        """
        print('Drawing queue (PEND/RUN) job num curve for queue "' + str(queue) + '".')

        if self.queueDbFileConnectResult == 'failed':
            common.printWarning('*Warning*: Failed on connectiong queue database file "' + str(self.queueDbFile) + '".')
            return

        dateList = []
        pendList = []
        runList = []
        tempPendList = []
        tempRunList = []

        if self.queueFirstLoad:
            common.printWarning('*Warning*: It is the first time loading queue database, it may cost a little time ...')
            self.queueFirstLoad = False

        tableName = 'queue_' + str(queue)
        dataDic = sqlite3_common.getSqlTableData(self.queueDbFile, self.queueDbConn, tableName, ['sampleTime', 'PEND', 'RUN'])

        if not dataDic:
            common.printWarning('*Warning*: queue information is missing for "' + str(queue) + '".')
            return
        else:
            origSampleTimeList = dataDic['sampleTime']
            origPendList = dataDic['PEND']
            origRunList = dataDic['RUN']

            for i in range(len(origSampleTimeList)):
                sampleTime = origSampleTimeList[i]
                date = re.sub('_.*', '', sampleTime)
                pendNum = origPendList[i]
                runNum = origRunList[i]

                if (i != 0) and ((i == len(origSampleTimeList)-1) or (date not in dateList)):
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

            # Cut dateList/pendList/runList, only save 15 days result.a
            if len(dateList) > 15:
                dateList = dateList[-15:]
                pendList = pendList[-15:]
                runList = runList[-15:]

            if len(dateList) == 0:
                common.printWarning('*Warning*: PEND/RUN job number information is missing for queue "' + str(queue) + '".')
                return
            else:
                queueJobNumCurveFig = str(config.tempPath) + '/' + str(self.user) + '_' + str(queue) + '_jobNum.png'
                queueNum = common.stringToInt(queue)

                print('Save queue PEND/RUN job numeber curve as "' + str(queueJobNumCurveFig) + '".')
                common.drawPlots(dateList, [pendList, runList], 'DATE', 'NUM', ['PEND', 'RUN'], xIsString=True, title='queue : ' + str(queue), saveName=queueJobNumCurveFig, figureNum=queueNum)

#################
# Main Function #
#################
def main():
    bmonitorGUI.main()

if __name__ == '__main__':
    main()
