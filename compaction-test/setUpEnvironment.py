__author__ = 'haotan'

import os

# change it later to parset it form partition file
MOUNT_PATH = "/tmp/0"
AMBRY_CONFIG_PATH = "./demo/"


def clean_up_partitions():
    print "Cleaning storage directory\n"
    cmd_line = "rm " + MOUNT_PATH + "/*"
    os.system(cmd_line)


def start_up_ambry():
    print "Starting up ambry server"
    server_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties -jar ../target/ambry.jar " \
                 "--serverPropsFilePath ../config/server.properties " \
                 "--hardwareLayoutFilePath ../config/HardwareLayout.json " \
                 "--partitionLayoutFilePath ./demo/PartitionLayout.json > " \
                 "../logs/server.log & echo $! >> save_pid.txt"

    frontend_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties " \
                   "-cp \"../target/*\" com.github.ambry.frontend.AmbryFrontendMain " \
                   "--serverPropsFilePath ../config/frontend.properties " \
                   "--hardwareLayoutFilePath ../config/HardwareLayout.json " \
                   "--partitionLayoutFilePath ./demo/PartitionLayout.json > " \
                   "../logs/frontend.log & echo $! >> save_pid.txt"
    os.system(server_cmd)
    os.system(frontend_cmd)


if __name__ == '__main__':
    clean_up_partitions()
    print "Finish cleaning up old data\n"
    start_up_ambry()
    print "Server started.."

