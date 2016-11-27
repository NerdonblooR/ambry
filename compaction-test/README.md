- How to monitor live metrics?
1. Install VisualVM
https://visualvm.github.io/download.html

2. Install VisualVM-MBeans plugin
Open VisualVM -> Tools -> Plugin -> Select VisualVM-Means -> Click Install

3. Start the Ambry server. Note the JMX server info in the command line output. It looks like this:
[2016-11-27 13:27:09,693] INFO Started jmx serverJmxServer port= 57356 url= service:jmx:rmi:///jndi/rmi://MAC-10001145:57356/jmxrmi (com.github.ambry.metrics.JmxServer)
"service:jmx:rmi:///jndi/rmi://MAC-10001145:57356/jmxrmi" is the JMX server URL.

4. Connect VisualVM to JMX server
Open VisualVM -> File -> Add JMX Connection -> Input the JMX server URL from step 3 into 'Connection'. -> Check 'Do not require SSL connection' -> OK

5. Monitor metrics from MBean
Double click on the new JMX connection you just created -> goto MBeans tab -> goto metrics
Now you can inspect all Ambry metrics live.
