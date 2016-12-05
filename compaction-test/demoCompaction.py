import threading
import Queue
import os
import random
import time
import base64
import struct

# 1GB filename
BIG_FILE = "DEMOPIC.JPG"
# 100mb filename
MID_FILE = "midFile"
# 10MB filename
SMALL_FILE = "smallFile"
# 100KB filename
TINY_FILE = "tinyFile"
# Put rate for each file
BIG_RATE = 0.01
MID_RATE = 0.1
SMALL_RATE = 0.4
# Shuffle period
SHUFFLE_TIME = 30
# Operation rate
READ_RATE = 0.8
WRITE_RATE = 0.18
DELETE_RATE = 1 - READ_RATE - WRITE_RATE
# Partition pool hit rate
HOT_HIT_RATE = 0.7
WARM_HIT_RATE = 0.2
# Partition type rate
HOT_RATE = 0.1
WARM_RATE = 0.2


def reportPerPartition(hotPartitions, warmPartitions, coldPartitions):
    metricPath = "/tmp/metrics"
    testResultPath = "/tmp/testResult"
    getResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StoreGetResponse.csv"
    putResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StorePutResponse.csv"
    outPutFile = testResultPath + "/{0}Partition[{1}].TestReport.csv"
    partitionTypes = ["hot", "warm", "cold"]
    partitionGroups = [hotPartitions, warmPartitions, coldPartitions]
    metricFiles = [getResponseFile, putResponseFile]
    colToPreserve = [[1, 2, 3], [1, 2, 3]]

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
                    result_q.put(self._putBlob(fileName))
                else:
                    blobId = job[1]
                    self._delete(blobId)
            except Queue.Empty:
                continue

    def join(self, timeout=None):
        self.stopRequest.set()
        super(WorkerThread, self).join(timeout)

    def _getBlob(self, blobId):
        print "Worker: " + str(self.tid) + " do get\n"
        cmdLine = "curl http://localhost:1174/{0}".format(blobId)
        print cmdLine
        os.system(cmdLine)

    def _putBlob(self, fileName):
        print "\nWorker: " + str(self.tid) + " do put\n"
        cmdLine = "curl -i -H \"x-ambry-blob-size : " \
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
        cmdLine = "curl -i -X DELETE http://localhost:1174/{0}".format(blobId)
        os.system(cmdLine)


class MasterThread(threading.Thread):
    """Master that assigns job to wokers"""

    def __init__(self, input_q, result_q, blobMap):
        super(MasterThread, self).__init__()
        self.input_q = input_q
        self.result_q = result_q
        self.stopRequest = threading.Event()

        # blobMap[i] = list blobs stored in partition i
        self.blobMap = list(blobMap)
        self.partitionNum = len(blobMap)
        self._populatePartitionPool()

    def run(self):
        prevTime = time.time()
        while not self.stopRequest.isSet():
            currTime = time.time()
            if currTime - prevTime >= SHUFFLE_TIME:
                self._populatePartitionPool()
                prevTime = currTime

            try:
                # Worker will append response to result queue after retrieving response from put operation
                # response [partitionId, blobId]
                response = self.result_q.get(True, 0.05)
                partitionId = response[0]
                blobId = response[1]
                self.blobMap[partitionId].append(blobId)
                self._assignJob()

            # get response from result_q
            except Queue.Empty:
                self._assignJob()
                continue


    def join(self, timeout=None):
        self.stopRequest.set()
        super(MasterThread, self).join(timeout)

    def _assignJob(self):
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
        self.warmPartition = range(hotNum, min(hotNum + warNum, partitionNum))
        self.coldPartition = range(min(hotNum + warNum, partitionNum), partitionNum)


if __name__ == '__main__':
    testDuration = 10
    # populate ambry with random files
    partitionNum = 2
    bigFileNum = 15
    midFileNum = 0
    smallFileNum = 0
    tinyFileNum = 0

    fileNumList = [bigFileNum, midFileNum, smallFileNum, tinyFileNum]
    fileNameList = [BIG_FILE, MID_FILE, SMALL_FILE, TINY_FILE]

    workerNum = 1
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
    for i in range(10):
        input_q.put(["put", BIG_FILE])

    blobIdList = []
    # Retrive response from result_queue to build blobMap
    while len(blobIdList) < 10:
        response = result_q.get()
        blobId = response[1]
        blobIdList.append(blobId)

    print("Finish Loading\n")
    print os.popen("ls -alh /tmp/0").read()

    print("Deleting until compaction will be triggered...\n")

    for i in range(6):
        blobId = blobIdList[i]
        cmdLine = "curl -i -X DELETE http://localhost:1174/{0}".format(blobId)
        os.system(cmdLine)
        print "Blob[{0}] get deleted\n".format(blobId)

    print "Delete one from following list to trigger compaction: \n"
    print blobIdList[6:]

    print os.popen("ls -alh /tmp/0").read()









