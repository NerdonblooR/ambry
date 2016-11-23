import threading
import Queue
import os
import random
import time

# 10GB filename
BIG_FILE = "bigFile"
# 1GB filename
MID_FILE = "midFile"
# 10MB filename
SMALL_FILE = "smallFile"
# 500KB filename
TINY_FILE = "tinyFile"
# Put rate for each file
BIG_RATE = 0.01
MID_RATE = 0.1
SMALL_RATE = 0.4
# Shuffle period
SHUFFLE_TIME = 30
# Operation rate
READ_RATE = 0.8
WRITE_RATE = 0.15
DELETE_RATE = 1 - READ_RATE - WRITE_RATE
# Partition pool hit rate
HOT_HIT_RATE = 0.7
WARM_HIT_RATE = 0.2
# Partition type rate
HOT_RATE = 0.1
WARM_RATE = 0.2


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
        pass

    def _putBlob(self, fileName):
        print "Worker: " + str(self.tid) + " do put\n"
        cmdLine = ""
        retLine = os.popen(cmdLine).read()
        # parse the line
        response = list()  # [partitionId, blobID]
        return response

    def _delete(self, blobId):
        print "Worker: " + str(self.tid) + " do delete\n"
        pass


class MasterThread(threading.Thread):
    """docstring for MasterThread"""

    def __init__(self, input_q, result_q, blobMap):
        super(MasterThread, self).__init__()
        self.input_q = input_q
        self.result_q = result_q
        self.stopRequest = threading.Event()

        # blobMap[i] = list blobs stored in partition i
        self.blobMap = blobMap
        self.partitionNum = len(blobMap)
        self._populatePartitionPool()

    def run(self):
        prevTime = time.time()
        while not self.stopRequest.isSet():
            currTime = time.time()
            if currTime - prevTime >= SHUFFLE_TIME:
                self._populatePartitionPool()
                prevTime = currTime
            # generate job ticket
            job = list()
            job.append(self._pickJob())
            if job[0] == "put":
                job.append(self._pickFile)
            elif job[0] == "get":
                partitionId = self._pickPartition()
                job.append(random.choice(self.blobMap[partitionId]))
            else:
                # choose a random blob to delete
                partition = random.choice(blobMap)
                blob = random.choice(partition)
                partition.remove(blob)
                job.append(blob)

            self.input_q.put(job)

            try:
                # Worker will append response to result queue after retrieving response from put operation
                # response [partitionId, blobId]
                response = self.result_q.get(False)
                partitionId = response[0]
                blobId = response[1]
                blobMap[partitionId].append(blobId)
            # get response from result_q
            except Queue.Empty:
                continue

    def join(self, timeout=None):
        self.stopRequest.set()
        super(WorkerThread, self).join(timeout)

    def _pickPartition(self):
        r = random.random()
        if r <= HOT_HIT_RATE:
            p = random.choice(self.hotPartition)
        elif HOT_HIT_RATE < r <= HOT_HIT_RATE + WARM_HIT_RATE:
            p = random.choice(self.warmPartition)
        else:
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
            return MID_RATE
        elif BIG_RATE + MID_RATE < r <= BIG_RATE + MID_RATE + SMALL_RATE:
            return SMALL_RATE
        else:
            return TINY_FILE

    def _populatePartitionPool(self):
        self.hotPartition = list()
        self.warmPartition = list()
        self.coldPartition = list()
        partitionIdList = [i for i in range(self.partitionNum)]
        hotNum = int(HOT_RATE * self.partitionNum)
        warmNum = int(WARM_RATE * self.partitionNum)
        for i in range(hotNum):
            picked = random.choice(partitionIdList)
            self.hotPartition.append(picked)
            partitionIdList.remove(picked)

        for j in range(warmNum):
            picked = random.choice(partitionIdList)
            self.warmPartition.append(picked)
            partitionIdList.remove(picked)

        self.coldPartition += partitionIdList


if __name__ == '__main__':
    testDuration = 0
    # populate ambry with random files
    partitionNum = 10
    bigFileNum = 100
    midFileNum = 100
    smallFileNum = 100
    tinyFileNum = 100

    fileNumList = [bigFileNum, midFileNum, smallFileNum, tinyFileNum]
    fileNameList = [BIG_FILE, MID_FILE, SMALL_FILE, TINY_FILE]

    workerNum = 3
    blobMap = [[] for i in range(partitionNum)]

    # pending job size is limit to 5000
    input_q = Queue.Queue(5000)
    result_q = Queue.Queue()
    # Create the "thread pool"
    pool = [WorkerThread(tid=i, input_q=input_q, result_q=result_q) for i in range(workerNum)]
    # Start the worker
    for worker in pool:
        worker.start()

    print("Start Loading\n")

    # Load blobs into ambry
    for i in range(len(fileNameList)):
        for j in range(fileNameList[i]):
            input_q.put(["put", fileNameList[i]])

    print("Finish Loading\n")

    # Retrive response from result_queue to build blobMap
    while not result_q.empty():
        response = result_q.get(False)
        partitionId = response[0]
        blobId = response[1]
        blobMap[partitionId].append(blobId)

    print("Finish Building BlobMap\n")

    # Create a master
    master = MasterThread(input_q, result_q, blobMap)
    master.start()

    # Looping till test time exhausted
    jobStartTime = time.time()
    while True:
        curTime = time.time()
        if testDuration != 0 and curTime - jobStartTime > testDuration:
            master.join()
            for worker in pool:
                worker.join()







