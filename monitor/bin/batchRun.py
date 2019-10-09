#!PYTHONPATH
# -*- coding: utf-8 -*-
#
# NOTICE:
# 1. Make sure the script execution account can ssh all of the openlava hosts without password.
# 2. Execute this script on crontab (Interval : 5 minutes).

import os
import sys
import argparse
import threading
import time

sys.path.append('MONITORPATH')
from common import common
from common import openlava_common
from conf import config

os.environ["PYTHONUNBUFFERED"]="1"

def readArgs():
    """
    Read arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-H", "--hosts",
                        nargs='+',
                        default=[],
                        help='Specify hosts to check.')

    args = parser.parse_args()
    return(args.hosts)

def runJobResourceSample(specifiedHostList):
    bhostsDic = openlava_common.getBhostsInfo()
    hostList = bhostsDic['HOST_NAME']
    runList = bhostsDic['RUN']
    threadList = []

    for i in range(len(hostList)):
        hostName = hostList[i]
        if (len(specifiedHostList) > 0) and (hostName not in specifiedHostList):
            continue

        runJobNum = runList[i]

        if int(runJobNum) > 0:
            jobResourceSample = str(config.installPath) + '/monitor/bin/jobResourceSample.py'
            print(str(hostName) + ' : ' + str(jobResourceSample))
            myThread = threading.Thread(target=common.sshRun, args=(hostName, jobResourceSample, 1200))
            threadList.append(myThread)
            myThread.setDaemon(True)
            myThread.start()
            time.sleep(1)
        else:
            print('*Info*: No job on host "' + str(hostName) + '", will ignore it.')

    for myThread in threadList:
        myThread.join()

################
# Main Process #
################
def main():
    (hostList) = readArgs()
    runJobResourceSample(hostList)

if __name__ == '__main__':
    main()
