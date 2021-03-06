#Instruction

##Download code:

$ git clone https://github.com/NerdonblooR/ambry.git
	
Most of the code for compaction and compaction scheduling is in:       
ambry/ambry-store/src/main/java/com.github.ambry.store/BlobStore.java

##Build Ambry: 

$ cd ambry      
$ ./gradlew allJar       
$ mkdir logs     

##Set up metrics dump directory:
$ mkdir /tmp/metrics

##Run tests with our scripts:

$ cd ambry/compaction-test    
$ python highAccessRate.py -s 0 -t 2 -w 5 -j 1000 --hardwareLayoutFilePath "./demo/HardwareLayout.json"  --partitionLayoutFilePath "./demo/PartitionLayout.json" --serverPropertiesPath "./testConfig/config" --bigFileNum 50 --midFileNum 50 --smallFileNum 0 --tinyFileNum 0 --metricPath "/tmp/metrics" --resultPath "./testResult/1" --partitionSize 1024        

The script will start Ambry frontend component and backend component, conduct tests and collect performance metrics.
When script stops, you can check ambry/compaction-test/testResult/1 for collected performance data

ambry/compaction-test/testResult/1 will contain:     
-csv files contain performance metrics for each partition  
-server.log contains debug info about compaction code:        
   check compaction elapsed time via:        
   $ cat server.log | grep Compaction             
-settings for the test: HardwareLayout.json, PartitionLayout.json, config             


If the script is running or does not stop normally, you can kill all the background processes created by the script via:        
$ cd ambry/compaction-test    
$ kill -9 \`cat save_pid.txt\`       


##Explaination for script parameters:     
-w \<worker threds number per process>   
-j \<number of requests the main process should fetch before it stops>   
-s \<1: enable shuffle partition pools, 0: disable shuffle partition pools>     
--hardwareLayoutFilePath \<hardwareConfigFile>      
--partitionLayoutFilePath \<partitionConfigFile>     
--serverPropertiesPath \<file contains the scheduling thresholds>   
--bigFileNum \<the number of big file a process need to load into ambry before test phase>   
--midFileNum \<the number of medium file a process need to load into ambry before test phase>   
--smallFileNum \<the number of small file a process need to load into ambry before test phase>   
--tinyFileNum \<the number of tiny file a process need to load into ambry before test phase>  
--metricPath \<must set to /tmp/metrics>        
--resultPath \<dir to store the test results when test stop>     
--partitionSize \<size in MB>     

Note that:
-Based on provided hardware layout file and partition layout file, Ambry will create 3 partitions
under /tmp/0, /tmp/1 and /tmp/2.            

Please also refer to: https://github.com/linkedin/ambry/wiki/Quick%20Start
if you want to interact with Ambry without our script
