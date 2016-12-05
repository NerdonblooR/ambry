__author__ = 'haotan'

import base64
import struct
import os


def addPadding(s):
    return s + ((len(s) / 3 + 1) * 3 - len(s)) * "="


def reportOverAllPerformance(blobMap):
    metricPath = "/tmp/metrics"
    getResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StoreGetResponse.csv"
    putResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StorePutResponse.csv"

    metricFiles = [getResponseFile, putResponseFile]
    metricNames = [["TotalGetResponseTime", "MeanResponseTime"], ["TotalPutResponseTime", "MeanPutResponseTime"]]
    metrics = {}

    for name in metricNames:
        metrics[name] = 0

    for i in range(len(blobMap)):
        for j in range(len(metricFiles)):
            file = open(metricFiles[j].format(i), 'r')
            # read to the last
            lastLine = file.readlines()[-1]
            lineList = lastLine.split(',')
            print metricFiles[j].format(i)
            print lineList
            count = float(lineList[1])
            mean = float(lineList[3])
            print count
            print mean
            metrics[metricNames[i]] += count * mean
    print metrics


def reportPerPartition(hotPartitions, warmPartitions, coldPartitions):
    metricPath = "/tmp/metrics"
    testResultPath = "/tmp/testResult"
    getResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StoreGetResponse.csv"
    putResponseFile = metricPath + "/com.github.ambry.store.BlobStore.Partition[{0}].StorePutResponse.csv"
    outPutFile = testResultPath + "/{0}Partition[{1}].TestReport.csv"
    partitionTypes = ["hot", "warm", "cold"]
    partitionGroups = [hotPartitions, warmPartitions, coldPartitions]
    metricFiles = [getResponseFile, putResponseFile]
    colToPreserve = [[1, 2, 3,12], [1, 2, 3,12]]

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




if __name__ == '__main__':
    # cmdLine = "curl -i -H \"x-ambry-blob-size : " \
    # "`wc -c {0} | xargs | cut -d\" \" -f1`\" " \
    #               "-H \"x-ambry-service-id : CUrlUpload\"  -H \"" \
    #               "x-ambry-owner-id : `whoami`\" -H \"x-ambry-content-type : image/jpg\" " \
    #               "-H \"x-ambry-um-description : Demonstration Image\" " \
    #               "http://localhost:1174/ --data-binary @{0}".format("lisp_cycles.png")
    #
    # retLine = os.popen(cmdLine).read()
    #
    # print retLine.split("\n")[1]
    blobID = "AAEAAQAAAAAAAAABAAAAJDRiZTNlNzE4LTkwMmMtNDNlZi05YzEwLTM2NTE2NDM3YzVjYw"
    print addPadding(blobID)
    partiotnID = base64.b64decode(addPadding(blobID))[4:12]
    print struct.unpack(">q", partiotnID)[0]
    reportPerPartition([0], [1], [])


    #os.system("touch test.py")






