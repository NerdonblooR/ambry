__author__ = 'haotan'

import os

# change it later to parset it form partition file
MOUNT_PATH = "/tmp/0"
AMBRY_CONFIG_PATH = "./demo/"


def start_up_ambry(hardwareLayout, partitionLayout):
    print "Starting up ambry server"
    server_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties -jar ../target/ambry.jar " \
                 "--serverPropsFilePath ../config/server.properties " \
                 "--hardwareLayoutFilePath {0}" \
                 "--partitionLayoutFilePath {1} > " \
                 "../logs/server.log & echo $! >> save_pid.txt".format(hardwareLayout, partitionLayout)

    frontend_cmd = "nohup java -Dlog4j.configuration=file:../config/log4j.properties " \
                   "-cp \"../target/*\" com.github.ambry.frontend.AmbryFrontendMain " \
                   "--serverPropsFilePath ../config/frontend.properties " \
                   "--hardwareLayoutFilePath {0} " \
                   "--partitionLayoutFilePath {1} > " \
                   "../logs/frontend.log & echo $! >> save_pid.txt".format(hardwareLayout, partitionLayout)
    print server_cmd
    print frontend_cmd

    os.system(server_cmd)
    os.system(frontend_cmd)


if __name__ == '__main__':
    #clean_up_partitions()
    print "Finish cleaning up old data\n"
    start_up_ambry("../config/HardwareLayout.json", "./demo/PartitionLayout.json")
    print "Server started.."
