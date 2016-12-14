import threading
import Queue
import sys, getopt
import os
import random
import time
import base64
import struct
import json

# 1GB filename
BIG_FILE = "bigFile"
# 100mb filename
MID_FILE = "midFile"
# 10MB filename
SMALL_FILE = "smallFile"
# 100KB filename
TINY_FILE = "tinyFile"
# Put rate for each file
BIG_RATE = 0.5
MID_RATE = 0.5
SMALL_RATE = 0
# Shuffle period
SHUFFLE_TIME = 30
# Operation rate
READ_RATE = 0.7
WRITE_RATE = 0.15
DELETE_RATE = 1 - READ_RATE - WRITE_RATE
# Partition pool hit rate
HOT_HIT_RATE = 0.7
WARM_HIT_RATE = 0.2
# Partition type rate
HOT_RATE = 0.1
WARM_RATE = 0.2


def cleanUpPartitions(mountPath, pID):
    cmd_line = "rm  {0}/{1}/*".format(mountPath, pID)
    print "Cleaning storage directory: {0}\n".format(cmd_line)
    os.system(cmd_line)


def start_up_ambry(hardwareLayout, partitionLayout, serverProperties, testResultPath):
    print "Starting up ambry server"
    server_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties -jar ../target/ambry.jar " \
                 "--serverPropsFilePath {2} " \
                 "--hardwareLayoutFilePath {0} " \
                 "--partitionLayoutFilePath {1} > " \
                 "../logs/server.log & echo $! >> save_pid.txt".format(hardwareLayout, partitionLayout,
                                                                       serverProperties)

    frontend_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties " \
                   "-cp \"../target/*\" com.github.ambry.frontend.AmbryFrontendMain " \
                   "--serverPropsFilePath ../config/frontend.properties " \
                   "--hardwareLayoutFilePath {0} " \
                   "--partitionLayoutFilePath {1} > " \
                   "../logs/frontend.log & echo $! >> save_pid.txt".format(hardwareLayout, partitionLayout)

    cpy_cmd = "cp {0} {1} {2} {3}".format(hardwareLayout, partitionLayout, serverProperties, testResultPath)
    vmstat_cmd = "nohup vmstat 5 1000 > {0}/vmstat.out & echo $! >> save_pid.txt".format(testResultPath)

    print server_cmd
    print frontend_cmd

    os.system(server_cmd)
    os.system(frontend_cmd)
    os.system(cpy_cmd)
    os.system(vmstat_cmd)


def stop_ambry(testResultPath):
    print "Stopping ambry...\n"
    os.system("kill -9 `cat save_pid.txt`;rm save_pid.txt")
    os.system("cp ../logs/server.log {0}".format(testResultPath))


def reportPerPartition(metricPath, testResultPath, hotPartitions, warmPartitions, coldPartitions):
    print "Reporting test result..."
    getResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StoreGetResponse.csv"
    putResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StorePutResponse.csv"
    outPutFile = testResultPath + "/{0}Partition[{1}].TestReport.csv"
    partitionTypes = ["hot", "warm", "cold"]
    partitionGroups = [hotPartitions, warmPartitions, coldPartitions]
    metricFiles = [getResponseFile, putResponseFile]
    colToPreserve = [[1, 2, 3, 12, 13], [1, 2, 3, 12, 13]]

    for i in range(len(partitionTypes)):
        for partiotnID in partitionGroups[i]:
            outfields = ["timeStamp"]
            outDict = {}
            resultFile = open(outPutFile.format(partitionTypes[i], partiotnID), 'w')

            f_idx = 0
            for fileName in metricFiles:
                f = open(fileName.format(partiotnID), 'r')
                # read the first line
                fields = f.readline().split(',')
                for c_idx in colToPreserve[f_idx]:
                    outfields.append(fileName.split(".")[-2] + "." + fields[c_idx])

                for line in f:
                    lineList = line.split(",")
                    timeStamp = int(lineList[0])
                    if f_idx == 0:
                        outDict[timeStamp] = []
                    for c_idx in colToPreserve[f_idx]:
                        outDict[timeStamp].append(lineList[c_idx])

                f.close()
                f_idx += 1

            timeStamps = outDict.keys()
            timeStamps.sort()
            resultFile.write(",".join(outfields) + "\n")
            for t in timeStamps:
                resultFile.write(str(t) + "," + ",".join(outDict[t]) + "\n")


def addPadding(s):
    """
    add padding to a base64 encoding
    """
    return s + ((len(s) / 3 + 1) * 3 - len(s)) * "="


class WorkerThread(threading.Thread):
    """docstring for ClassName"""

    def __init__(self, tid, input_q, result_q):
        super(WorkerThread, self).__init__()
        self.tid = tid
        self.input_q = input_q
        self.result_q = result_q
        self.stopRequest = threading.Event()

    def run(self):
        while not self.stopRequest.isSet():
            try:
                job = self.input_q.get(True, 0.05)
                if job[0] == "get":
                    blobId = job[1]
                    self._getBlob(blobId)
                elif job[0] == "put":
                    fileName = job[1]
                    self.result_q.put(self._putBlob(fileName))
                else:
                    blobId = job[1]
                    self._delete(blobId)
            except:
                continue

    def join(self, timeout=None):
        self.stopRequest.set()
        super(WorkerThread, self).join(timeout)

    def _getBlob(self, blobId):
        print "Worker: " + str(self.tid) + " do get\n"
        cmdLine = "curl -s http://localhost:1174/{0}".format(blobId)
        print cmdLine
        os.system(cmdLine)

    def _putBlob(self, fileName):
        print "Worker: " + str(self.tid) + " do put\n"
        cmdLine = "curl -s -i -H \"x-ambry-blob-size : " \
                  "`wc -c {0} | xargs | cut -d\" \" -f1`\" " \
                  "-H \"x-ambry-service-id : CUrlUpload\"  -H \"" \
                  "x-ambry-owner-id : `whoami`\" -H \"x-ambry-content-type : image/jpg\" " \
                  "-H \"x-ambry-um-description : Demonstration Image\" " \
                  "http://localhost:1174/ --data-binary @{0}".format(fileName)

        retLine = os.popen(cmdLine).read()
        blobId = retLine.split("\n")[1][11:-1]
        partitionId = struct.unpack(">q", base64.b64decode(addPadding(blobId))[4:12])[0]

        response = list()  # [partitionId, blobID]
        response.append(partitionId)
        response.append(blobId)
        return response

    def _delete(self, blobId):
        print "Worker: " + str(self.tid) + " do delete\n"
        cmdLine = "curl -s -i -X DELETE http://localhost:1174/{0}".format(blobId)
        os.system(cmdLine)


class MasterThread(threading.Thread):
    """Master that assigns job to wokers"""

    def __init__(self, jobNum, input_q, result_q, blobMap, needShuffle):
        super(MasterThread, self).__init__()
        self.jobNum = jobNum
        self.input_q = input_q
        self.result_q = result_q
        self.stopRequest = threading.Event()

        # blobMap[i] = list blobs stored in partition i
        self.blobMap = list(blobMap)
        self.partitionNum = len(blobMap)
        self._populatePartitionPool()

    def run(self):
        # TODO: double check ending criteria
        lastTime = time.time()
        while self.jobNum > 0 or not self.input_q.empty():
            if self.needShuffle == 1:
                now = time.time()
                if lastTime - now > 90:
                    self._shufflePartitionPool()
                    lastTime = now



            try:
                # Worker will append response to result queue after retrieving response from put operation
                # response [partitionId, blobId]
                response = self.result_q.get(True, 0.05)
                partitionId = response[0]
                blobId = response[1]
                if partitionId < self.partitionNum:
                    self.blobMap[partitionId].append(blobId)
                self._assignJob()

            # get response from result_q
            except:
                self._assignJob()
                continue

    def join(self, timeout=None):
        super(MasterThread, self).join(timeout)

    def _assignJob(self):
        if self.jobNum != 0:
            # assign job
            job = list()
            job.append(self._pickJob())
            if job[0] == "put":
                job.append(self._pickFile())
            elif job[0] == "get":
                partitionId = self._pickPartition()
                # fix the case when there is nothing in the partition
                if len(self.blobMap[partitionId]) == 0:
                    return
                job.append(random.choice(self.blobMap[partitionId]))
            else:
                if min(map(len, self.blobMap)) == 0:
                    return
                # choose a random blob to delete
                partition = random.choice(self.blobMap)
                blob = random.choice(partition)
                # remove the blob id from the partition
                partition.remove(blob)
                job.append(blob)
            self.jobNum -= 1
            self.input_q.put(job)

    """
    pick a partition for getting blobs
    """

    def _pickPartition(self):
        r = random.random()
        hot = filter(lambda x: len(self.blobMap[x]) > 0, self.hotPartition)
        warm = filter(lambda x: len(self.blobMap[x]) > 0, self.warmPartition)
        cold = filter(lambda x: len(self.blobMap[x]) > 0, self.coldPartition)

        p = 0
        if r <= HOT_HIT_RATE and len(hot) > 0:
            p = random.choice(hot)
        elif HOT_HIT_RATE < r <= HOT_HIT_RATE + WARM_HIT_RATE and len(warm) > 0:
            p = random.choice(self.warmPartition)
        elif len(cold) > 0:
            p = random.choice(self.coldPartition)
        return p

    def _pickJob(self):
        r = random.random()
        if r <= READ_RATE:
            jobType = "get"
        elif READ_RATE < r <= READ_RATE + WRITE_RATE:
            jobType = "put"
        else:
            jobType = "delete"

        return jobType

    def _pickFile(self):
        r = random.random()
        if r <= BIG_RATE:
            return BIG_FILE
        elif BIG_RATE < r <= BIG_RATE + MID_RATE:
            return MID_FILE
        elif BIG_RATE + MID_RATE < r <= BIG_RATE + MID_RATE + SMALL_RATE:
            return SMALL_FILE
        else:
            return TINY_FILE

    """
    randomly populate hot, warm and cold partition pool
    """

    def _populatePartitionPool(self):
        partitionIdList = [i for i in range(self.partitionNum)]
        hotNum = max(1, int(HOT_RATE * self.partitionNum))
        warmNum = max(1, int(WARM_RATE * self.partitionNum))
        self.hotPartition = range(0, hotNum)
        self.warmPartition = range(hotNum, min(hotNum + warmNum, self.partitionNum))
        self.coldPartition = range(min(hotNum + warmNum, self.partitionNum), self.partitionNum)

    """
    shuffle hot, warm and cold partition pool
    """

    def _shufflePartitionPool(self):
        newCold = self.hotPartition.pop()
        newWarm = self.coldPartition.pop()
        newHot = self.warmPartition.pop()

        self.hotPartition.append(newHot)
        self.warmPartition.append(newWarm)
        self.coldPartition.append(newCold)


def usage():
    print 'compactionTest.py -t <extra thread num> -w <workerNumber> -j <jobNumber> -s <shufflebit> ' \
          '--hardwareLayoutFilePath <hardwareConfigFile>  ' \
          '--partitionLayoutFilePath <partitionConfigFile> ' \
          '--serverPropertiesPath <serverPropertiesFile>' \
          '--bigFileNum <bigFileNum> --midFileNum <midFileNum> ' \
          '--smallFileNum <smallFileNum> --tinyFileNum <tinyFileNum> ' \
          '--metricPath <metricPath> --resultPath <resultPath> ' \
          '--partitionSize <sizeInMB>'


def main(argv):
    if len(argv) == 0:
        usage()
        sys.exit(2)

    try:
        opts, args = getopt.getopt(argv, "ht:w:j:s:",
                                   ["hardwareLayoutFilePath=",
                                    "partitionLayoutFilePath=",
                                    "serverPropertiesPath=",
                                    "bigFileNum=", "midFileNum=",
                                    "smallFileNum=", "tinyFileNum=", "metricPath=",
                                    "resultPath=", "partitionSize="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    extraThreadNum = 0
    workerNum = 0
    jobNum = 0
    bigFileNum = 0
    midFileNum = 0
    smallFileNum = 0
    tinyFileNum = 0
    partitionSizeMB = 0
    hardwareLayoutFile = ""
    partitionLayoutFile = ""
    serverPropertiesFile = ""
    metricPath = ""
    resultPath = ""
    needShuffle = 0

    for opt, arg in opts:
        if opt == "-h":
            usage()
            sys.exit()
        elif opt == "-w":
            workerNum = int(arg)
        elif opt == "-t":
            extraThreadNum = int(arg)
        elif opt == "-j":
            jobNum = int(arg)
        elif opt == "-s":
            needShuffle = int(arg)
        elif opt == "--bigFileNum":
            bigFileNum = int(arg)
        elif opt == "--midFileNum":
            midFileNum = int(arg)
        elif opt == "--smallFileNum":
            smallFileNum = int(arg)
        elif opt == "--tinyFileNum":
            tinyFileNum = int(arg)
        elif opt == "--partitionSize":
            partitionSizeMB = int(arg)
        elif opt == "--hardwareLayoutFilePath":
            hardwareLayoutFile = arg
        elif opt == "--partitionLayoutFilePath":
            partitionLayoutFile = arg
        elif opt == "--serverPropertiesPath":
            serverPropertiesFile = arg
        elif opt == "--metricPath":
            metricPath = arg
        elif opt == "--resultPath":
            resultPath = arg

    # parsing and modifying ambry partitionLayout file
    with open(partitionLayoutFile) as data_file:
        data = json.load(data_file)
        for p in data["partitions"]:
            # reset partition size
            p["replicaCapacityInBytes"] = partitionSizeMB * 1024 * 1024
            for r in p["replicas"]:
                cleanUpPartitions(r["mountPath"], p["id"])
    # save the modification to partitionLayout file
    with open(partitionLayoutFile, 'w') as data_file:
        json.dump(data, data_file)

    cmd_line = "rm  {0}/*".format(metricPath)
    print "Cleaning metric directory: {0}\n".format(cmd_line)
    os.system(cmd_line)

    cmd_line = "rm  {0}/*".format(resultPath)
    print "Cleaning result directory: {0}\n".format(cmd_line)
    os.system(cmd_line)

    partitionNum = len(data["partitions"])

    start_up_ambry(hardwareLayoutFile, partitionLayoutFile, serverPropertiesFile, resultPath)

    print "Starting performance tests...\n"
    time.sleep(5)

    doTest(extraThreadNum, workerNum, jobNum, partitionNum, bigFileNum, midFileNum, smallFileNum, tinyFileNum,
           metricPath, resultPath, needShuffle)

    stop_ambry(resultPath)


def doTest(threadNum, workerNum, jobNum, partitionNum, bigFileNum, midFileNum, smallFileNum, tinyFileNum, metricPath,
           resultPath, needShuffle):
    # start extra thread for higher access rate
    cmd_line = "nohup python workerScript.py -w {0} -j {1} -p {6} -s {7} " \
               "--bigFileNum {2} --midFileNum {3} " \
               "--smallFileNum {4} --tinyFileNum {5} > /dev/null & echo $! >> save_pid.txt".format(workerNum, jobNum,
                                                                                                   bigFileNum,
                                                                                                   midFileNum,
                                                                                                   smallFileNum,
                                                                                                   tinyFileNum,
                                                                                                   partitionNum,
                                                                                                   needShuffle)

    for i in range(threadNum):
        os.system(cmd_line)
    # populate ambry with random files


    fileNumList = [bigFileNum, midFileNum, smallFileNum, tinyFileNum]
    fileNameList = [BIG_FILE, MID_FILE, SMALL_FILE, TINY_FILE]
    blobMap = [[] for i in range(partitionNum)]
    input_q = Queue.Queue()
    result_q = Queue.Queue()
    # Create the "thread pool"
    pool = [WorkerThread(tid=i, input_q=input_q, result_q=result_q) for i in range(workerNum)]
    # Start the worker
    for worker in pool:
        worker.start()

    print("Start Loading\n")

    # Load blobs into ambry
    for i in range(len(fileNameList)):
        for j in range(fileNumList[i]):
            input_q.put(["put", fileNameList[i]])
    print("Finish Loading\n")

    fileCount = 0
    # Retrive response from result_queue to build blobMap
    while fileCount < sum(fileNumList):
        # not input_q.empty():
        response = result_q.get()
        partitionId = response[0]
        if partitionId < partitionNum:
            blobId = response[1]
            blobMap[partitionId].append(blobId)
        fileCount += 1
        print fileCount, "\n"

    print("Finish Building BlobMap\n")

    # Create a master
    master = MasterThread(jobNum, input_q, result_q, blobMap, needShuffle)
    master.start()

    # Looping till master stops
    while master.isAlive():
        continue

    for worker in pool:
        worker.join()

    reportPerPartition(metricPath, resultPath, master.hotPartition, master.warmPartition, master.coldPartition)
    # reportPerPartition(master.hotPartition, master.warmPartition, master.coldPartitions)
    print("Finish Test..\n")


if __name__ == '__main__':
    main(sys.argv[1:])
