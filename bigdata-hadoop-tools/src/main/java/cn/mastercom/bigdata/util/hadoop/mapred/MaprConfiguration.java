package cn.mastercom.bigdata.util.hadoop.mapred;

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class MaprConfiguration  extends Configuration
{
	protected static final Log LOG = LogFactory.getLog(MaprConfiguration.class);

	public MaprConfiguration(Configuration that)
	{	
		for (Entry<String, String> e : that)
		{
			set(e.getKey(), e.getValue());
		}
		
		initialize_270();
	}
	
	public MaprConfiguration(List<String> fileList)
	{
		super();
//		addResource(name);
	}

	public void initialize_270()
	{
//		//If job tracker is static the history files are stored in this single well known place. If No value is set here, by default, it is in the local file system at ${hadoop.log.dir}/history.
//		conf.set("mapreduce.jobtracker.jobhistory.location","");
//		// Every task attempt progresses from 0.0 to 1.0 [unless it fails or is killed]. We record, for each task attempt, certain statistics over each twelfth of the progress range. You can change the number of intervals we divide the entire range of progress into by setting this property. Higher values give more precision to the recorded data, but costs more memory in the job tracker at runtime. Each increment in this attribute costs 16 bytes per running task.
//		conf.set("mapreduce.jobtracker.jobhistory.task.numberprogresssplits","12");
//		// User can specify a location to store the history files of a particular job. If nothing is specified, the logs are stored in output directory. The files are stored in "_logs/history/" in the directory. User can stop logging by giving the value "none".
//		conf.set("mapreduce.job.userhistorylocation","");
//		// The completed job history files are stored at this single well known location. If nothing is specified, the files are stored at ${mapreduce.jobtracker.jobhistory.location}/done.
//		conf.set("mapreduce.jobtracker.jobhistory.completed.location","");
//		// true, if job needs job-setup and job-cleanup. false, otherwise
//		conf.set("mapreduce.job.committer.setup.cleanup.needed","TRUE");
//		//The number of streams to merge at once while sorting files. This determines the number of open file handles.
//		conf.set("mapreduce.task.io.sort.factor","10");
//		//The total amount of buffer memory to use while sorting files, in megabytes. By default, gives each merge stream 1MB, which should minimize seeks.
//		conf.set("mapreduce.task.io.sort.mb","100");
//		//The soft limit in the serialization buffer. Once reached, a thread will begin to spill the contents to disk in the background. Note that collection will not block if this threshold is exceeded while a spill is already in progress, so spills may be larger than this threshold when it is set to less than .5
//		conf.set("mapreduce.map.sort.spill.percent","0.8");
//		//The host and port that the MapReduce job tracker runs at. If "local", then jobs are run in-process as a single map and reduce task.
//		conf.set("mapreduce.jobtracker.address","local");
//		//This the client factory that is responsible for creating local job runner client
//		conf.set("mapreduce.local.clientfactory.class.name","org.apache.hadoop.mapred.LocalClientFactory");
//		// The job tracker http server address and port the server will listen on. If the port is 0 then the server will start on a free port.
//		conf.set("mapreduce.jobtracker.http.address","0.0.0.0:50030");
//		// The number of server threads for the JobTracker. This should be roughly 4% of the number of tasktracker nodes.
//		conf.set("mapreduce.jobtracker.handler.count","10");
//		//The interface and port that task tracker server listens on. Since it is only connected to by the tasks, it uses the local interface. EXPERT ONLY. Should only be changed if your host does not have the loopback interface.
//		conf.set("mapreduce.tasktracker.report.address","127.0.0.1:0");
//		//The local directory where MapReduce stores intermediate data files. May be a comma-separated list of directories on different devices in order to spread disk i/o. Directories that do not exist are ignored.
//		conf.set("mapreduce.cluster.local.dir","${hadoop.tmp.dir}/mapred/local");
//		//The directory where MapReduce stores control files.
//		conf.set("mapreduce.jobtracker.system.dir","${hadoop.tmp.dir}/mapred/system");
//		//The root of the staging area for users' job files In practice, this should be the directory where users' home directories are located (usually /user)
//		conf.set("mapreduce.jobtracker.staging.root.dir","${hadoop.tmp.dir}/mapred/staging");
//		//A shared directory for temporary files.
//		conf.set("mapreduce.cluster.temp.dir","${hadoop.tmp.dir}/mapred/temp");
//		//If the space in mapreduce.cluster.local.dir drops under this, do not ask for more tasks. Value in bytes.
//		conf.set("mapreduce.tasktracker.local.dir.minspacestart","0");
//		//If the space in mapreduce.cluster.local.dir drops under this, do not ask more tasks until all the current ones have finished and cleaned up. Also, to save the rest of the tasks we have running, kill one of them, to clean up some space. Start with the reduce tasks, then go with the ones that have finished the least. Value in bytes.
//		conf.set("mapreduce.tasktracker.local.dir.minspacekill","0");
//		//Expert: The time-interval, in miliseconds, after which a tasktracker is declared 'lost' if it doesn't send heartbeats.
//		conf.set("mapreduce.jobtracker.expire.trackers.interval","600000");
//		//Expert: The instrumentation class to associate with each TaskTracker.
//		conf.set("mapreduce.tasktracker.instrumentation","org.apache.hadoop.mapred.TaskTrackerMetricsInst");
//		// Name of the class whose instance will be used to query resource information on the tasktracker. The class must be an instance of org.apache.hadoop.util.ResourceCalculatorPlugin. If the value is null, the tasktracker attempts to use a class appropriate to the platform. Currently, the only platform supported is Linux.
//		conf.set("mapreduce.tasktracker.resourcecalculatorplugin","");
//		//The interval, in milliseconds, for which the tasktracker waits between two cycles of monitoring its tasks' memory usage. Used only if tasks' memory management is enabled via mapred.tasktracker.tasks.maxmemory.
//		conf.set("mapreduce.tasktracker.taskmemorymanager.monitoringinterval","5000");
//		//The time, in milliseconds, the tasktracker waits for sending a SIGKILL to a task, after it has been sent a SIGTERM. This is currently not used on WINDOWS where tasks are just sent a SIGTERM.
//		conf.set("mapreduce.tasktracker.tasks.sleeptimebeforesigkill","5000");
//		//The default number of map tasks per job. Ignored when mapreduce.jobtracker.address is "local".
//		conf.set("mapreduce.job.maps","2");
//		//The default number of reduce tasks per job. Typically set to 99% of the cluster's reduce capacity, so that if a node fails the reduces can still be executed in a single wave. Ignored when mapreduce.jobtracker.address is "local".
//		conf.set("mapreduce.job.reduces","1");
//		//true to enable (job) recovery upon restart, "false" to start afresh
//		conf.set("mapreduce.jobtracker.restart.recover","FALSE");
//		//The block size of the job history file. Since the job recovery uses job history, its important to dump job history to disk as soon as possible. Note that this is an expert level parameter. The default value is set to 3 MB.
//		conf.set("mapreduce.jobtracker.jobhistory.block.size","3145728");
//		//The class responsible for scheduling the tasks.
//		conf.set("mapreduce.jobtracker.taskscheduler","org.apache.hadoop.mapred.JobQueueTaskScheduler");
//		//The threshold in terms of seconds after which an unsatisfied mapper request triggers reducer preemption to free space. Default 0 implies that the reduces should be preempted immediately after allocation if there is currently no room for newly allocated mappers.
//		conf.set("mapreduce.job.reducer.preempt.delay.sec","0");
//		//The max number of block locations to store for each split for locality calculation.
//		conf.set("mapreduce.job.max.split.locations","10");
//		//The maximum permissible size of the split metainfo file. The JobTracker won't attempt to read split metainfo files bigger than the configured value. No limits if set to -1.
//		conf.set("mapreduce.job.split.metainfo.maxsize","10000000");
//		//The maximum number of running tasks for a job before it gets preempted. No limits if undefined.
//		conf.set("mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob","");
//		//Expert: The maximum number of attempts per map task. In other words, framework will try to execute a map task these many number of times before giving up on it.
//		conf.set("mapreduce.map.maxattempts","4");
//		//Expert: The maximum number of attempts per reduce task. In other words, framework will try to execute a reduce task these many number of times before giving up on it.
//		conf.set("mapreduce.reduce.maxattempts","4");
//		//Set to enable fetch retry during host restart.
//		conf.set("mapreduce.reduce.shuffle.fetch.retry.enabled","${yarn.nodemanager.recovery.enabled}");
//		//Time of interval that fetcher retry to fetch again when some non-fatal failure happens because of some events like NM restart.
//		conf.set("mapreduce.reduce.shuffle.fetch.retry.interval-ms","1000");
//		//Timeout value for fetcher to retry to fetch again when some non-fatal failure happens because of some events like NM restart.
//		conf.set("mapreduce.reduce.shuffle.fetch.retry.timeout-ms","30000");
//		//The maximum number of ms the reducer will delay before retrying to download map data.
//		conf.set("mapreduce.reduce.shuffle.retry-delay.max.ms","60000");
//		//The default number of parallel transfers run by reduce during the copy(shuffle) phase.
//		conf.set("mapreduce.reduce.shuffle.parallelcopies","5");
//		//Expert: The maximum amount of time (in milli seconds) reduce task spends in trying to connect to a tasktracker for getting map output.
//		conf.set("mapreduce.reduce.shuffle.connect.timeout","180000");
//		//Expert: The maximum amount of time (in milli seconds) reduce task waits for map output data to be available for reading after obtaining connection.
//		conf.set("mapreduce.reduce.shuffle.read.timeout","180000");
//		//set to true to support keep-alive connections.
//		conf.set("mapreduce.shuffle.connection-keep-alive.enable","FALSE");
//		//The number of seconds a shuffle client attempts to retain http connection. Refer "Keep-Alive: timeout=" header in Http specification
//		conf.set("mapreduce.shuffle.connection-keep-alive.timeout","5");
//		//The number of milliseconds before a task will be terminated if it neither reads an input, writes an output, nor updates its status string. A value of 0 disables the timeout.
//		conf.set("mapreduce.task.timeout","600000");
//		//The maximum number of map tasks that will be run simultaneously by a task tracker.
//		conf.set("mapreduce.tasktracker.map.tasks.maximum","2");
//		//The maximum number of reduce tasks that will be run simultaneously by a task tracker.
//		conf.set("mapreduce.tasktracker.reduce.tasks.maximum","2");
//		//The amount of memory to request from the scheduler for each map task.
//		conf.set("mapreduce.map.memory.mb","1024");
//		//The number of virtual cores to request from the scheduler for each map task.
//		conf.set("mapreduce.map.cpu.vcores","1");
//		//The amount of memory to request from the scheduler for each reduce task.
//		conf.set("mapreduce.reduce.memory.mb","1024");
//		//The number of virtual cores to request from the scheduler for each reduce task.
//		conf.set("mapreduce.reduce.cpu.vcores","1");
//		//The number of retired job status to keep in the cache.
//		conf.set("mapreduce.jobtracker.retiredjobs.cache.size","1000");
//		//Expert: Set this to true to let the tasktracker send an out-of-band heartbeat on task-completion for better latency.
//		conf.set("mapreduce.tasktracker.outofband.heartbeat","FALSE");
//		//The number of job history files loaded in memory. The jobs are loaded when they are first accessed. The cache is cleared based on LRU.
//		conf.set("mapreduce.jobtracker.jobhistory.lru.cache.size","5");
//		//Expert: The instrumentation class to associate with each JobTracker.
//		conf.set("mapreduce.jobtracker.instrumentation","org.apache.hadoop.mapred.JobTrackerMetricsInst");
//		//Java opts for the task processes. The following symbol, if present, will be interpolated: @taskid@ is replaced by current TaskID. Any other occurrences of '@' will go unchanged. For example, to enable verbose gc logging to a file named for the taskid in /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of: -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc Usage of -Djava.library.path can cause programs to no longer function if hadoop native libraries are used. These values should instead be set as part of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and mapreduce.reduce.env config settings.
//		conf.set("mapred.child.java.opts","#NAME?");
//		//User added environment variables for the task processes. Example : 1) A=foo This will set the env variable A to foo 2) B=$B:c This is inherit nodemanager's B env variable on Unix. 3) B=%B%;c This is inherit nodemanager's B env variable on Windows.
//		conf.set("mapred.child.env","");
//		// Expert: Additional execution environment entries for map and reduce task processes. This is not an additive property. You must preserve the original value if you want your map and reduce tasks to have access to native libraries (compression, etc). When this value is empty, the command to set execution envrionment will be OS dependent: For linux, use LD_LIBRARY_PATH=$HADOOP_COMMON_HOME/lib/native. For windows, use PATH = %PATH%;%HADOOP_COMMON_HOME%\\bin.
//		conf.set("mapreduce.admin.user.env","");
//		// To set the value of tmp directory for map and reduce tasks. If the value is an absolute path, it is directly assigned. Otherwise, it is prepended with task's working directory. The java tasks are executed with option -Djava.io.tmpdir='the absolute path of the tmp dir'. Pipes and streaming are set with environment variable, TMPDIR='the absolute path of the tmp dir'
//		conf.set("mapreduce.task.tmp.dir","./tmp");
//		//The logging level for the map task. The allowed levels are: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
//		conf.set("mapreduce.map.log.level","INFO");
//		//The logging level for the reduce task. The allowed levels are: OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
//		conf.set("mapreduce.reduce.log.level","INFO");
//		// The number of virtual cores required for each map task.
//		conf.set("mapreduce.map.cpu.vcores","1");
//		// The number of virtual cores required for each reduce task.
//		conf.set("mapreduce.reduce.cpu.vcores","1");
//		//The threshold, in terms of the number of files for the in-memory merge process. When we accumulate threshold number of files we initiate the in-memory merge and spill to disk. A value of 0 or less than 0 indicates we want to DON'T have any threshold and instead depend only on the ramfs's memory consumption to trigger the merge.
//		conf.set("mapreduce.reduce.merge.inmem.threshold","1000");
//		//The usage threshold at which an in-memory merge will be initiated, expressed as a percentage of the total memory allocated to storing in-memory map outputs, as defined by mapreduce.reduce.shuffle.input.buffer.percent.
//		conf.set("mapreduce.reduce.shuffle.merge.percent","0.66");
//		//The percentage of memory to be allocated from the maximum heap size to storing map outputs during the shuffle.
//		conf.set("mapreduce.reduce.shuffle.input.buffer.percent","0.7");
//		//The percentage of memory- relative to the maximum heap size- to retain map outputs during the reduce. When the shuffle is concluded, any remaining map outputs in memory must consume less than this threshold before the reduce can begin.
//		conf.set("mapreduce.reduce.input.buffer.percent","0");
//		//Expert: Maximum percentage of the in-memory limit that a single shuffle can consume
//		conf.set("mapreduce.reduce.shuffle.memory.limit.percent","0.25");
//		// Whether to use SSL for for the Shuffle HTTP endpoints.
//		conf.set("mapreduce.shuffle.ssl.enabled","FALSE");
//		//Buffer size for reading spills from file when using SSL.
//		conf.set("mapreduce.shuffle.ssl.file.buffer.size","65536");
//		//Max allowed connections for the shuffle. Set to 0 (zero) to indicate no limit on the number of connections.
//		conf.set("mapreduce.shuffle.max.connections","0");
//		//Max allowed threads for serving shuffle connections. Set to zero to indicate the default of 2 times the number of available processors (as reported by Runtime.availableProcessors()). Netty is used to serve requests, so a thread is not needed for each connection.
//		conf.set("mapreduce.shuffle.max.threads","0");
//		//This option can enable/disable using nio transferTo method in the shuffle phase. NIO transferTo does not perform well on windows in the shuffle phase. Thus, with this configuration property it is possible to disable it, in which case custom transfer method will be used. Recommended value is false when running Hadoop on Windows. For Linux, it is recommended to set it to true. If nothing is set then the default value is false for Windows, and true for Linux.
//		conf.set("mapreduce.shuffle.transferTo.allowed","");
//		//This property is used only if mapreduce.shuffle.transferTo.allowed is set to false. In that case, this property defines the size of the buffer used in the buffer copy code for the shuffle phase. The size of this buffer determines the size of the IO requests.
//		conf.set("mapreduce.shuffle.transfer.buffer.size","131072");
//		//The percentage of memory -relative to the maximum heap size- to be used for caching values when using the mark-reset functionality.
//		conf.set("mapreduce.reduce.markreset.buffer.percent","0");
//		//If true, then multiple instances of some map tasks may be executed in parallel.
//		conf.set("mapreduce.map.speculative","TRUE");
//		//If true, then multiple instances of some reduce tasks may be executed in parallel.
//		conf.set("mapreduce.reduce.speculative","TRUE");
//		//The max percent (0-1) of running tasks that can be speculatively re-executed at any time.
//		conf.set("mapreduce.job.speculative.speculativecap","0.1");
//		// The MapOutputCollector implementation(s) to use. This may be a comma-separated list of class names, in which case the map task will try to initialize each of the collectors in turn. The first to successfully initialize will be used.
//		conf.set("mapreduce.job.map.output.collector.class","org.apache.hadoop.mapred.MapTask$MapOutputBuffer");
//		//The number of standard deviations by which a task's ave progress-rates must be lower than the average of all running tasks' for the task to be considered too slow.
//		conf.set("mapreduce.job.speculative.slowtaskthreshold","1");
//		//The number of standard deviations by which a Task Tracker's ave map and reduce progress-rates (finishTime-dispatchTime) must be lower than the average of all successful map/reduce task's for the TT to be considered too slow to give a speculative task to.
//		conf.set("mapreduce.job.speculative.slownodethreshold","1");
//		//How many tasks to run per jvm. If set to -1, there is no limit.
//		conf.set("mapreduce.job.jvm.numtasks","1");
//		//Whether to enable the small-jobs "ubertask" optimization, which runs "sufficiently small" jobs sequentially within a single JVM. "Small" is defined by the following maxmaps, maxreduces, and maxbytes settings. Note that configurations for application masters also affect the "Small" definition - yarn.app.mapreduce.am.resource.mb must be larger than both mapreduce.map.memory.mb and mapreduce.reduce.memory.mb, and yarn.app.mapreduce.am.resource.cpu-vcores must be larger than both mapreduce.map.cpu.vcores and mapreduce.reduce.cpu.vcores to enable ubertask. Users may override this value.
//		conf.set("mapreduce.job.ubertask.enable","FALSE");
//		//Threshold for number of maps, beyond which job is considered too big for the ubertasking optimization. Users may override this value, but only downward.
//		conf.set("mapreduce.job.ubertask.maxmaps","9");
//		//Threshold for number of reduces, beyond which job is considered too big for the ubertasking optimization. CURRENTLY THE CODE CANNOT SUPPORT MORE THAN ONE REDUCE and will ignore larger values. (Zero is a valid max, however.) Users may override this value, but only downward.
//		conf.set("mapreduce.job.ubertask.maxreduces","1");
//		//Threshold for number of input bytes, beyond which job is considered too big for the ubertasking optimization. If no value is specified, dfs.block.size is used as a default. Be sure to specify a default value in mapred-site.xml if the underlying filesystem is not HDFS. Users may override this value, but only downward.
//		conf.set("mapreduce.job.ubertask.maxbytes","");
//		//Specifies if the Application Master should emit timeline data to the timeline server. Individual jobs can override this value.
//		conf.set("mapreduce.job.emit-timeline-data","FALSE");
//		//The minimum size chunk that map input should be split into. Note that some file formats may have minimum split sizes that take priority over this setting.
//		conf.set("mapreduce.input.fileinputformat.split.minsize","0");
//		//The number of threads to use to list and fetch block locations for the specified input paths. Note: multiple threads should not be used if a custom non thread-safe path filter is used.
//		conf.set("mapreduce.input.fileinputformat.list-status.num-threads","1");
//		//The maximum number of tasks for a single job. A value of -1 indicates that there is no maximum.
//		conf.set("mapreduce.jobtracker.maxtasks.perjob","-1");
//		//When using NLineInputFormat, the number of lines of input data to include in each split.
//		conf.set("mapreduce.input.lineinputformat.linespermap","1");
//		//The replication level for submitted job files. This should be around the square root of the number of nodes.
//		conf.set("mapreduce.client.submit.file.replication","10");
//		//The name of the Network Interface from which a task tracker should report its IP address.
//		conf.set("mapreduce.tasktracker.dns.interface","default");
//		//The host name or IP address of the name server (DNS) which a TaskTracker should use to determine the host name used by the JobTracker for communication and display purposes.
//		conf.set("mapreduce.tasktracker.dns.nameserver","default");
//		//The number of worker threads that for the http server. This is used for map output fetching
//		conf.set("mapreduce.tasktracker.http.threads","40");
//		// The task tracker http server address and port. If the port is 0 then the server will start on a free port.
//		conf.set("mapreduce.tasktracker.http.address","0.0.0.0:50060");
//		//Should the files for failed tasks be kept. This should only be used on jobs that are failing, because the storage is never reclaimed. It also prevents the map outputs from being erased from the reduce directory as they are consumed.
//		conf.set("mapreduce.task.files.preserve.failedtasks","FALSE");
//		//Should the job outputs be compressed?
//		conf.set("mapreduce.output.fileoutputformat.compress","FALSE");
//		//If the job outputs are to compressed as SequenceFiles, how should they be compressed? Should be one of NONE, RECORD or BLOCK.
//		conf.set("mapreduce.output.fileoutputformat.compress.type","RECORD");
//		//If the job outputs are compressed, how should they be compressed?
//		conf.set("mapreduce.output.fileoutputformat.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
//		//Should the outputs of the maps be compressed before being sent across the network. Uses SequenceFile compression.
//		conf.set("mapreduce.map.output.compress","FALSE");
//		//If the map outputs are compressed, how should they be compressed?
//		conf.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.DefaultCodec");
//		//The default sort class for sorting keys.
//		conf.set("map.sort.class","org.apache.hadoop.util.QuickSort");
//		//The maximum size of user-logs of each task in KB. 0 disables the cap.
//		conf.set("mapreduce.task.userlog.limit.kb","0");
//		//The maximum size of the MRAppMaster attempt container logs in KB. 0 disables the cap.
//		conf.set("yarn.app.mapreduce.am.container.log.limit.kb","0");
//		//Number of backup files for task logs when using ContainerRollingLogAppender (CRLA). See org.apache.log4j.RollingFileAppender.maxBackupIndex. By default, ContainerLogAppender (CLA) is used, and container logs are not rolled. CRLA is enabled for tasks when both mapreduce.task.userlog.limit.kb and yarn.app.mapreduce.task.container.log.backups are greater than zero.
//		conf.set("yarn.app.mapreduce.task.container.log.backups","0");
//		//Number of backup files for the ApplicationMaster logs when using ContainerRollingLogAppender (CRLA). See org.apache.log4j.RollingFileAppender.maxBackupIndex. By default, ContainerLogAppender (CLA) is used, and container logs are not rolled. CRLA is enabled for the ApplicationMaster when both mapreduce.task.userlog.limit.kb and yarn.app.mapreduce.am.container.log.backups are greater than zero.
//		conf.set("yarn.app.mapreduce.am.container.log.backups","0");
//		//The maximum time, in hours, for which the user-logs are to be retained after the job completion.
//		conf.set("mapreduce.job.userlog.retain.hours","24");
//		//Names a file that contains the list of nodes that may connect to the jobtracker. If the value is empty, all hosts are permitted.
//		conf.set("mapreduce.jobtracker.hosts.filename","");
//		//Names a file that contains the list of hosts that should be excluded by the jobtracker. If the value is empty, no hosts are excluded.
//		conf.set("mapreduce.jobtracker.hosts.exclude.filename","");
//		//Expert: Approximate number of heart-beats that could arrive at JobTracker in a second. Assuming each RPC can be processed in 10msec, the default value is made 100 RPCs in a second.
//		conf.set("mapreduce.jobtracker.heartbeats.in.second","100");
//		//The number of blacklists for a taskTracker by various jobs after which the task tracker could be blacklisted across all jobs. The tracker will be given a tasks later (after a day). The tracker will become a healthy tracker after a restart.
//		conf.set("mapreduce.jobtracker.tasktracker.maxblacklists","4");
//		//The number of task-failures on a tasktracker of a given job after which new tasks of that job aren't assigned to it. It MUST be less than mapreduce.map.maxattempts and mapreduce.reduce.maxattempts otherwise the failed task will never be tried on a different node.
//		conf.set("mapreduce.job.maxtaskfailures.per.tracker","3");
//		//The filter for controlling the output of the task's userlogs sent to the console of the JobClient. The permissible options are: NONE, KILLED, FAILED, SUCCEEDED and ALL.
//		conf.set("mapreduce.client.output.filter","FAILED");
//		//The interval (in milliseconds) between which the JobClient polls the JobTracker for updates about job status. You may want to set this to a lower value to make tests run faster on a single node system. Adjusting this value in production may lead to unwanted client-server traffic.
//		conf.set("mapreduce.client.completion.pollinterval","5000");
//		//The interval (in milliseconds) between which the JobClient reports status to the console and checks for job completion. You may want to set this to a lower value to make tests run faster on a single node system. Adjusting this value in production may lead to unwanted client-server traffic.
//		conf.set("mapreduce.client.progressmonitor.pollinterval","1000");
//		//Indicates if persistency of job status information is active or not.
//		conf.set("mapreduce.jobtracker.persist.jobstatus.active","TRUE");
//		//The number of hours job status information is persisted in DFS. The job status information will be available after it drops of the memory queue and between jobtracker restarts. With a zero value the job status information is not persisted at all in DFS.
//		conf.set("mapreduce.jobtracker.persist.jobstatus.hours","1");
//		//The directory where the job status information is persisted in a file system to be available after it drops of the memory queue and between jobtracker restarts.
//		conf.set("mapreduce.jobtracker.persist.jobstatus.dir","/jobtracker/jobsInfo");
//		//To set whether the system should collect profiler information for some of the tasks in this job? The information is stored in the user log directory. The value is "true" if task profiling is enabled.
//		conf.set("mapreduce.task.profile","FALSE");
//		// To set the ranges of map tasks to profile. mapreduce.task.profile has to be set to true for the value to be accounted.
//		conf.set("mapreduce.task.profile.maps","0-2");
//		// To set the ranges of reduce tasks to profile. mapreduce.task.profile has to be set to true for the value to be accounted.
//		conf.set("mapreduce.task.profile.reduces","0-2");
//		//JVM profiler parameters used to profile map and reduce task attempts. This string may contain a single format specifier %s that will be replaced by the path to profile.out in the task attempt log directory. To specify different profiling options for map tasks and reduce tasks, more specific parameters mapreduce.task.profile.map.params and mapreduce.task.profile.reduce.params should be used.
//		conf.set("mapreduce.task.profile.params","-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s");
//		//Map-task-specific JVM profiler parameters. See mapreduce.task.profile.params
//		conf.set("mapreduce.task.profile.map.params","${mapreduce.task.profile.params}");
//		//Reduce-task-specific JVM profiler parameters. See mapreduce.task.profile.params
//		conf.set("mapreduce.task.profile.reduce.params","${mapreduce.task.profile.params}");
//		// The number of Task attempts AFTER which skip mode will be kicked off. When skip mode is kicked off, the tasks reports the range of records which it will process next, to the TaskTracker. So that on failures, TT knows which ones are possibly the bad records. On further executions, those are skipped.
//		conf.set("mapreduce.task.skip.start.attempts","2");
//		// The flag which if set to true, SkipBadRecords.COUNTER_MAP_PROCESSED_RECORDS is incremented by MapRunner after invoking the map function. This value must be set to false for applications which process the records asynchronously or buffer the input records. For example streaming. In such cases applications should increment this counter on their own.
//		conf.set("mapreduce.map.skip.proc.count.autoincr","TRUE");
//		// The flag which if set to true, SkipBadRecords.COUNTER_REDUCE_PROCESSED_GROUPS is incremented by framework after invoking the reduce function. This value must be set to false for applications which process the records asynchronously or buffer the input records. For example streaming. In such cases applications should increment this counter on their own.
//		conf.set("mapreduce.reduce.skip.proc.count.autoincr","TRUE");
//		// If no value is specified here, the skipped records are written to the output directory at _logs/skip. User can stop writing skipped records by giving the value "none".
//		conf.set("mapreduce.job.skip.outdir","");
//		// The number of acceptable skip records surrounding the bad record PER bad record in mapper. The number includes the bad record as well. To turn the feature of detection/skipping of bad records off, set the value to 0. The framework tries to narrow down the skipped range by retrying until this threshold is met OR all attempts get exhausted for this task. Set the value to Long.MAX_VALUE to indicate that framework need not try to narrow down. Whatever records(depends on application) get skipped are acceptable.
//		conf.set("mapreduce.map.skip.maxrecords","0");
//		// The number of acceptable skip groups surrounding the bad group PER bad group in reducer. The number includes the bad group as well. To turn the feature of detection/skipping of bad groups off, set the value to 0. The framework tries to narrow down the skipped range by retrying until this threshold is met OR all attempts get exhausted for this task. Set the value to Long.MAX_VALUE to indicate that framework need not try to narrow down. Whatever groups(depends on application) get skipped are acceptable.
//		conf.set("mapreduce.reduce.skip.maxgroups","0");
//		//Configuration key to enable/disable IFile readahead.
//		conf.set("mapreduce.ifile.readahead","TRUE");
//		//Configuration key to set the IFile readahead length in bytes.
//		conf.set("mapreduce.ifile.readahead.bytes","4194304");
//		// This is the max level of the task cache. For example, if the level is 2, the tasks cached are at the host level and at the rack level.
//		conf.set("mapreduce.jobtracker.taskcache.levels","2");
//		// Queue to which a job is submitted. This must match one of the queues defined in mapred-queues.xml for the system. Also, the ACL setup for the queue must allow the current user to submit a job to the queue. Before specifying a queue, ensure that the system is configured with the queue, and access is allowed for submitting jobs to the queue.
//		conf.set("mapreduce.job.queuename","default");
//		// Tags for the job that will be passed to YARN at submission time. Queries to YARN for applications can filter on these tags.
//		conf.set("mapreduce.job.tags","");
//		// Specifies whether ACLs should be checked for authorization of users for doing various queue and job level operations. ACLs are disabled by default. If enabled, access control checks are made by JobTracker and TaskTracker when requests are made by users for queue operations like submit job to a queue and kill a job in the queue and job operations like viewing the job-details (See mapreduce.job.acl-view-job) or for modifying the job (See mapreduce.job.acl-modify-job) using Map/Reduce APIs, RPCs or via the console and web user interfaces. For enabling this flag(mapreduce.cluster.acls.enabled), this is to be set to true in mapred-site.xml on JobTracker node and on all TaskTracker nodes.
//		conf.set("mapreduce.cluster.acls.enabled","FALSE");
//		// Job specific access-control list for 'modifying' the job. It is only used if authorization is enabled in Map/Reduce by setting the configuration property mapreduce.cluster.acls.enabled to true. This specifies the list of users and/or groups who can do modification operations on the job. For specifying a list of users and groups the format to use is "user1,user2 group1,group". If set to '*', it allows all users/groups to modify this job. If set to ' '(i.e. space), it allows none. This configuration is used to guard all the modifications with respect to this job and takes care of all the following operations: o killing this job o killing a task of this job, failing a task of this job o setting the priority of this job Each of these operations are also protected by the per-queue level ACL "acl-administer-jobs" configured via mapred-queues.xml. So a caller should have the authorization to satisfy either the queue-level ACL or the job-level ACL. Irrespective of this ACL configuration, (a) job-owner, (b) the user who started the cluster, (c) members of an admin configured supergroup configured via mapreduce.cluster.permissions.supergroup and (d) queue administrators of the queue to which this job was submitted to configured via acl-administer-jobs for the specific queue in mapred-queues.xml can do all the modification operations on a job. By default, nobody else besides job-owner, the user who started the cluster, members of supergroup and queue administrators can perform modification operations on a job.
//		conf.set("mapreduce.job.acl-modify-job","");
//		// Job specific access-control list for 'viewing' the job. It is only used if authorization is enabled in Map/Reduce by setting the configuration property mapreduce.cluster.acls.enabled to true. This specifies the list of users and/or groups who can view private details about the job. For specifying a list of users and groups the format to use is "user1,user2 group1,group". If set to '*', it allows all users/groups to modify this job. If set to ' '(i.e. space), it allows none. This configuration is used to guard some of the job-views and at present only protects APIs that can return possibly sensitive information of the job-owner like o job-level counters o task-level counters o tasks' diagnostic information o task-logs displayed on the TaskTracker web-UI and o job.xml showed by the JobTracker's web-UI Every other piece of information of jobs is still accessible by any other user, for e.g., JobStatus, JobProfile, list of jobs in the queue, etc. Irrespective of this ACL configuration, (a) job-owner, (b) the user who started the cluster, (c) members of an admin configured supergroup configured via mapreduce.cluster.permissions.supergroup and (d) queue administrators of the queue to which this job was submitted to configured via acl-administer-jobs for the specific queue in mapred-queues.xml can do all the view operations on a job. By default, nobody else besides job-owner, the user who started the cluster, memebers of supergroup and queue administrators can perform view operations on a job.
//		conf.set("mapreduce.job.acl-view-job","");
//		// The maximum memory that a task tracker allows for the index cache that is used when serving map outputs to reducers.
//		conf.set("mapreduce.tasktracker.indexcache.mb","10");
//		//Whether to write tracking ids of tokens to job-conf. When true, the configuration property "mapreduce.job.token.tracking.ids" is set to the token-tracking-ids of the job
//		conf.set("mapreduce.job.token.tracking.ids.enabled","FALSE");
//		//When mapreduce.job.token.tracking.ids.enabled is set to true, this is set by the framework to the token-tracking-ids used by the job.
//		conf.set("mapreduce.job.token.tracking.ids","");
//		// The number of records to process during merge before sending a progress notification to the TaskTracker.
//		conf.set("mapreduce.task.merge.progress.records","10000");
//		// The number of records to process during combine output collection before sending a progress notification.
//		conf.set("mapreduce.task.combine.progress.records","10000");
//		//Fraction of the number of maps in the job which should be complete before reduces are scheduled for the job.
//		conf.set("mapreduce.job.reduce.slowstart.completedmaps","0.05");
//		// if false - do not unregister/cancel delegation tokens from renewal, because same tokens may be used by spawned jobs
//		conf.set("mapreduce.job.complete.cancel.delegation.tokens","TRUE");
//		//TaskController which is used to launch and manage task execution
//		conf.set("mapreduce.tasktracker.taskcontroller","org.apache.hadoop.mapred.DefaultTaskController");
//		//Expert: Group to which TaskTracker belongs. If LinuxTaskController is configured via mapreduce.tasktracker.taskcontroller, the group owner of the task-controller binary should be same as this group.
//		conf.set("mapreduce.tasktracker.group","");
//		//Default port that the ShuffleHandler will run on. ShuffleHandler is a service run at the NodeManager to facilitate transfers of intermediate Map outputs to requesting Reducers.
//		conf.set("mapreduce.shuffle.port","13562");
//		// Name of the class whose instance will be used to send shuffle requests by reducetasks of this job. The class must be an instance of org.apache.hadoop.mapred.ShuffleConsumerPlugin.
//		conf.set("mapreduce.job.reduce.shuffle.consumer.plugin.class","org.apache.hadoop.mapreduce.task.reduce.Shuffle");
//		//Absolute path to the script which is periodicallyrun by the node health monitoring service to determine if the node is healthy or not. If the value of this key is empty or the file does not exist in the location configured here, the node health monitoring service is not started.
//		conf.set("mapreduce.tasktracker.healthchecker.script.path","");
//		//Frequency of the node health script to be run, in milliseconds
//		conf.set("mapreduce.tasktracker.healthchecker.interval","60000");
//		//Time after node health script should be killed if unresponsive and considered that the script has failed.
//		conf.set("mapreduce.tasktracker.healthchecker.script.timeout","600000");
//		//List of arguments which are to be passed to node health script when it is being launched comma seperated.
//		conf.set("mapreduce.tasktracker.healthchecker.script.args","");
//		//Limit on the number of user counters allowed per job.
//		conf.set("mapreduce.job.counters.limit","120");
//		//The runtime framework for executing MapReduce jobs. Can be one of local, classic or yarn.
//		conf.set("mapreduce.framework.name","local");
//		//The staging dir used while submitting jobs.
//		conf.set("yarn.app.mapreduce.am.staging-dir","/tmp/hadoop-yarn/staging");
//		//The maximum number of application attempts. It is a application-specific setting. It should not be larger than the global number set by resourcemanager. Otherwise, it will be override. The default number is set to 2, to allow at least one retry for AM.
//		conf.set("mapreduce.am.max-attempts","2");
//		//Indicates url which will be called on completion of job to inform end status of job. User can give at most 2 variables with URI : $jobId and $jobStatus. If they are present in URI, then they will be replaced by their respective values.
//		conf.set("mapreduce.job.end-notification.url","");
//		//The number of times the submitter of the job wants to retry job end notification if it fails. This is capped by mapreduce.job.end-notification.max.attempts
//		conf.set("mapreduce.job.end-notification.retry.attempts","0");
//		//The number of milliseconds the submitter of the job wants to wait before job end notification is retried if it fails. This is capped by mapreduce.job.end-notification.max.retry.interval
//		conf.set("mapreduce.job.end-notification.retry.interval","1000");
//		//The maximum number of times a URL will be read for providing job end notification. Cluster administrators can set this to limit how long after end of a job, the Application Master waits before exiting. Must be marked as final to prevent users from overriding this.
//		conf.set("mapreduce.job.end-notification.max.attempts","5");
//		//The maximum amount of time (in milliseconds) to wait before retrying job end notification. Cluster administrators can set this to limit how long the Application Master waits before exiting. Must be marked as final to prevent users from overriding this.
//		conf.set("mapreduce.job.end-notification.max.retry.interval","5000");
//		//User added environment variables for the MR App Master processes. Example : 1) A=foo This will set the env variable A to foo 2) B=$B:c This is inherit tasktracker's B env variable.
//		conf.set("yarn.app.mapreduce.am.env","");
//		// Environment variables for the MR App Master processes for admin purposes. These values are set first and can be overridden by the user env (yarn.app.mapreduce.am.env) Example : 1) A=foo This will set the env variable A to foo 2) B=$B:c This is inherit app master's B env variable.
//		conf.set("yarn.app.mapreduce.am.admin.user.env","");
//		//Java opts for the MR App Master processes. The following symbol, if present, will be interpolated: @taskid@ is replaced by current TaskID. Any other occurrences of '@' will go unchanged. For example, to enable verbose gc logging to a file named for the taskid in /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of: -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc Usage of -Djava.library.path can cause programs to no longer function if hadoop native libraries are used. These values should instead be set as part of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and mapreduce.reduce.env config settings.
//		conf.set("yarn.app.mapreduce.am.command-opts","#NAME?");
//		//Java opts for the MR App Master processes for admin purposes. It will appears before the opts set by yarn.app.mapreduce.am.command-opts and thus its options can be overridden user. Usage of -Djava.library.path can cause programs to no longer function if hadoop native libraries are used. These values should instead be set as part of LD_LIBRARY_PATH in the map / reduce JVM env using the mapreduce.map.env and mapreduce.reduce.env config settings.
//		conf.set("yarn.app.mapreduce.am.admin-command-opts","");
//		//The number of threads used to handle RPC calls in the MR AppMaster from remote tasks
//		conf.set("yarn.app.mapreduce.am.job.task.listener.thread-count","30");
//		//Range of ports that the MapReduce AM can use when binding. Leave blank if you want all possible ports. For example 50000-50050,50100-50200
//		conf.set("yarn.app.mapreduce.am.job.client.port-range","");
//		//The amount of time in milliseconds to wait for the output committer to cancel an operation if the job is killed
//		conf.set("yarn.app.mapreduce.am.job.committer.cancel-timeout","60000");
//		//Defines a time window in milliseconds for output commit operations. If contact with the RM has occurred within this window then commits are allowed, otherwise the AM will not allow output commits until contact with the RM has been re-established.
//		conf.set("yarn.app.mapreduce.am.job.committer.commit-window","10000");
//		//The interval in ms at which the MR AppMaster should send heartbeats to the ResourceManager
//		conf.set("yarn.app.mapreduce.am.scheduler.heartbeat.interval-ms","1000");
//		//The number of client retries to the AM - before reconnecting to the RM to fetch Application Status.
//		conf.set("yarn.app.mapreduce.client-am.ipc.max-retries","3");
//		//The number of client retries on socket timeouts to the AM - before reconnecting to the RM to fetch Application Status.
//		conf.set("yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts","3");
//		//The number of client retries to the RM/HS before throwing exception. This is a layer above the ipc.
//		conf.set("yarn.app.mapreduce.client.max-retries","3");
//		//The amount of memory the MR AppMaster needs.
//		conf.set("yarn.app.mapreduce.am.resource.mb","1536");
//		// The number of virtual CPU cores the MR AppMaster needs.
//		conf.set("yarn.app.mapreduce.am.resource.cpu-vcores","1");
//		//CLASSPATH for MR applications. A comma-separated list of CLASSPATH entries. If mapreduce.application.framework is set then this must specify the appropriate classpath for that archive, and the name of the archive must be present in the classpath. If mapreduce.app-submission.cross-platform is false, platform-specific environment vairable expansion syntax would be used to construct the default CLASSPATH entries. For Linux: $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*, $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*. For Windows: %HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/*, %HADOOP_MAPRED_HOME%/share/hadoop/mapreduce/lib/*. If mapreduce.app-submission.cross-platform is true, platform-agnostic default CLASSPATH for MR applications would be used: {{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/*, {{HADOOP_MAPRED_HOME}}/share/hadoop/mapreduce/lib/* Parameter expansion marker will be replaced by NodeManager on container launch based on the underlying OS accordingly.
//		conf.set("mapreduce.application.classpath","");
//		//If enabled, user can submit an application cross-platform i.e. submit an application from a Windows client to a Linux/Unix server or vice versa.
//		conf.set("mapreduce.app-submission.cross-platform","FALSE");
//		//Path to the MapReduce framework archive. If set, the framework archive will automatically be distributed along with the job, and this path would normally reside in a public location in an HDFS filesystem. As with distributed cache files, this can be a URL with a fragment specifying the alias to use for the archive name. For example, hdfs:/mapred/framework/hadoop-mapreduce-2.1.1.tar.gz#mrframework would alias the localized archive as "mrframework". Note that mapreduce.application.classpath must include the appropriate classpath for the specified framework. The base name of the archive, or alias of the archive if an alias is used, must appear in the specified classpath.
//		conf.set("mapreduce.application.framework.path","");
//		//Whether to use a separate (isolated) classloader for user classes in the task JVM.
//		conf.set("mapreduce.job.classloader","FALSE");
//		//Used to override the default definition of the system classes for the job classloader. The system classes are a comma-separated list of classes that should be loaded from the system classpath, not the user-supplied JARs, when mapreduce.job.classloader is enabled. Names ending in '.' (period) are treated as package names, and names starting with a '-' are treated as negative matches.
//		conf.set("mapreduce.job.classloader.system.classes","");
//		//MapReduce JobHistory Server IPC host:port
//		conf.set("mapreduce.jobhistory.address","0.0.0.0:10020");
//		//MapReduce JobHistory Server Web UI host:port
//		conf.set("mapreduce.jobhistory.webapp.address","0.0.0.0:19888");
//		// Location of the kerberos keytab file for the MapReduce JobHistory Server.
//		conf.set("mapreduce.jobhistory.keytab","/etc/security/keytab/jhs.service.keytab");
//		// Kerberos principal name for the MapReduce JobHistory Server.
//		conf.set("mapreduce.jobhistory.principal","jhs/_HOST@REALM.TLD");
//		//
//		conf.set("mapreduce.jobhistory.intermediate-done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done_intermediate");
//		//
//		conf.set("mapreduce.jobhistory.done-dir","${yarn.app.mapreduce.am.staging-dir}/history/done");
//		//
//		conf.set("mapreduce.jobhistory.cleaner.enable","TRUE");
//		// How often the job history cleaner checks for files to delete, in milliseconds. Defaults to 86400000 (one day). Files are only deleted if they are older than mapreduce.jobhistory.max-age-ms.
//		conf.set("mapreduce.jobhistory.cleaner.interval-ms","86400000");
//		// Job history files older than this many milliseconds will be deleted when the history cleaner runs. Defaults to 604800000 (1 week).
//		conf.set("mapreduce.jobhistory.max-age-ms","604800000");
//		//The number of threads to handle client API requests
//		conf.set("mapreduce.jobhistory.client.thread-count","10");
//		//Size of the date string cache. Effects the number of directories which will be scanned to find a job.
//		conf.set("mapreduce.jobhistory.datestring.cache.size","200000");
//		//Size of the job list cache
//		conf.set("mapreduce.jobhistory.joblist.cache.size","20000");
//		//Size of the loaded job cache
//		conf.set("mapreduce.jobhistory.loadedjobs.cache.size","5");
//		//Scan for history files to more from intermediate done dir to done dir at this frequency.
//		conf.set("mapreduce.jobhistory.move.interval-ms","180000");
//		//The number of threads used to move files.
//		conf.set("mapreduce.jobhistory.move.thread-count","3");
//		//The HistoryStorage class to use to cache history data.
//		conf.set("mapreduce.jobhistory.store.class","");
//		//Whether to use fixed ports with the minicluster
//		conf.set("mapreduce.jobhistory.minicluster.fixed.ports","FALSE");
//		//The address of the History server admin interface.
//		conf.set("mapreduce.jobhistory.admin.address","0.0.0.0:10033");
//		//ACL of who can be admin of the History server.
//		conf.set("mapreduce.jobhistory.admin.acl","*");
//		//Enable the history server to store server state and recover server state upon startup. If enabled then mapreduce.jobhistory.recovery.store.class must be specified.
//		conf.set("mapreduce.jobhistory.recovery.enable","FALSE");
//		//The HistoryServerStateStoreService class to store history server state for recovery.
//		conf.set("mapreduce.jobhistory.recovery.store.class","org.apache.hadoop.mapreduce.v2.hs.HistoryServerFileSystemStateStoreService");
//		//The URI where history server state will be stored if HistoryServerFileSystemStateStoreService is configured as the recovery storage class.
//		conf.set("mapreduce.jobhistory.recovery.store.fs.uri","${hadoop.tmp.dir}/mapred/history/recoverystore");
//		// This configures the HTTP endpoint for JobHistoryServer web UI. The following values are supported: - HTTP_ONLY : Service is provided only on http - HTTPS_ONLY : Service is provided only on https
//		conf.set("mapreduce.jobhistory.http.policy","HTTP_ONLY");
			
	}

	
	
}
