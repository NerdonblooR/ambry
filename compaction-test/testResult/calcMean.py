if __name__ == '__main__':
    hotFile = open("hotCompactTime.out", 'r')
    count = 0
    time = 0
    for line in hotFile:
        l = line.split(" ")
        time += float(l[-1][:-2])
        count += 1


    print "hotCompactTime.out: {0}".format(time/float(count))
