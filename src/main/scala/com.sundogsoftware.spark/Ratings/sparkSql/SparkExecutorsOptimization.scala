package com.sundogsoftware.spark.Ratings.sparkSql

class SparkExecutorsOptimization {
  /*
  *
  * To hopefully make all of this a little more concrete, here’s a worked example of configuring a Spark app to use as much of the cluster as possible:
  * Imagine a cluster with
  * six nodes running NodeManagers, each equipped with
  * 16 cores and
  * 64GB of memory.
  *
  * The NodeManager capacities, yarn.nodemanager.resource.memory-mb and yarn.nodemanager.resource.cpu-vcores, should probably be set to 63 * 1024 = 64512
   * (megabytes) and 15 respectively.
   * We avoid allocating 100% of the resources to YARN containers because the node needs some resources to run the OS and Hadoop daemons.
   * In this case, we leave a gigabyte and a core for these system processes.
   * Cloudera Manager helps by accounting for these and configuring these YARN properties automatically.

    The likely first impulse would be to use --num-executors 6 --executor-cores 15 --executor-memory 63G. However, this is the wrong approach because:

   * 63GB + the executor memory overhead won’t fit within the 63GB capacity of the NodeManagers. The application master will take up a core on one of the nodes,
   * meaning that there won’t be room for a 15-core executor on that node. 15 cores per executor can lead to bad HDFS I/O throughput.


   * A better option would be to use --num-executors 17 --executor-cores 5 --executor-memory 19G. Why?

   * This config results in three executors on all nodes except for the one with the AM, which will have two executors. --executor-memory was derived as
   * (63/3 executors per node) = 21. 21 * 0.07 = 1.47. 21 – 1.47 ~ 19.
   * --num-executors 17 - the total number of executors used for this job (SO this number will be divided across all of the nodes)
   * --executor-cores 5 - cores used in each executor
   * --executor-memory 19G - each executors memory usage.
   * In general the output of executor-cores * num-executors should be less than or equal to 6 (total nodes) * 16 (cores per node) = 96
  * */

  //Notes:
  //Hadoop/Yarn/OS Deamons: When we run spark application using a cluster manager like Yarn, there’ll be several daemons that’ll run in the background like NameNode,
  // Secondary NameNode, DataNode, JobTracker and TaskTracker. So, while specifying num-executors, we need to make sure that we leave aside enough cores (~1 core per node)
  // for these daemons to run smoothly.

  //Yarn ApplicationMaster (AM): ApplicationMaster is responsible for negotiating resources from the ResourceManager and working with the NodeManagers
  // to execute and monitor the containers and their resource consumption. If we are running spark on yarn, then we need to budget in the resources that AM
  // would need (~1024MB and 1 Executor).

  //HDFS Throughput: HDFS client has trouble with tons of concurrent threads. It was observed that HDFS achieves full write throughput with ~5 tasks per executor .
  // So it’s good to keep the number of cores per executor below that number.

    //So in a cluster with
  //* six nodes running NodeManagers, each equipped with
  //  * 16 cores and
  //  * 64GB of memory.
  // We can only use 15 cores  per node and
  // request only 60 GB memory in total, becaause we will need 7% memory overhead for each executor

  //explanation below (for extra memory)

  //Full memory requested to yarn per executor =
  //  spark-executor-memory + spark.yarn.executor.memoryOverhead.
  //    spark.yarn.executor.memoryOverhead =
  //    Max(384MB, 7% of spark.executor-memory)

  //So, if we request 20GB per executor, AM will actually get 20GB + memoryOverhead = 20 + 7% of 20GB = ~23GB memory for us.

  //Also Note :
  //Running executors with too much memory often results in excessive garbage collection delays.
  //Running tiny executors (with a single core and just enough memory needed to run a single task, for example) throws away the benefits that come from
  // running multiple tasks in a single JVM.

  //Example 2
  /*
  **Cluster Config:**
    10 Nodes
    16 cores per Node
    64GB RAM per Node

  First Approach - Tiny executors [One Executor per core]:

//Tiny executors essentially means one executor per core. Following table depicts the values of our spar-config params with this approach:

- `--num-executors`  = `In this approach, we'll assign one executor per core`
                     = `total-cores-in-cluster`
                     = `num-cores-per-node * total-nodes-in-cluster`
                     = 16 x 10 = 160
- `--executor-cores` = 1 (one executor per core)
- `--executor-memory` = `amount of memory per executor`
                      = `mem-per-node/num-executors-per-node`
                      = 64GB/16 = 4GB

Analysis: With only one executor per core, as we discussed above, we’ll not be able to take advantage of running multiple tasks in the same JVM.
Also, shared/cached variables like broadcast variables and accumulators will be replicated in each core of the nodes which is 16 times. Also, we are not
leaving enough memory overhead for Hadoop/Yarn daemon processes and we are not counting in ApplicationManager. NOT GOOD!

Second Approach -  Fat executors (One Executor per node):

Fat executors essentially means one executor per node. Following table depicts the values of our spark-config params with this approach:

- `--num-executors` = `In this approach, we'll assign one executor per node`
                    = `total-nodes-in-cluster`
                   = 10
- `--executor-cores` = `one executor per node means all the cores of the node are assigned to one executor`
                     = `total-cores-in-a-node`
                     = 16
- `--executor-memory` = `amount of memory per executor`
                     = `mem-per-node/num-executors-per-node`
                     = 64GB/1 = 64GB

//Analysis: With all 16 cores per executor, apart from ApplicationManager and daemon processes are not counted for, HDFS throughput will hurt and
it’ll result in excessive garbage results. Also,NOT GOOD!


Third Approach: Balance between Fat (vs) Tiny
According to the recommendations which we discussed above:


--num-executors = 29 (3 per node)
--executor-memory = 21 - 3 = 18GB
--executor-cores = 5

Based on the recommendations mentioned above, Let’s assign 5 core per executors => --executor-cores = 5 (for good HDFS throughput)
Leave 1 core per node for Hadoop/Yarn daemons => Num cores available per node = 16-1 = 15
So, Total available of cores in cluster = 15 x 10 = 150
Number of available executors = (total cores/num-cores-per-executor) = 150/5 = 30

Leaving 1 executor for ApplicationManager => --num-executors = 29
Number of executors per node = 30/10 = 3
Memory per executor = 64GB/3 = 21GB
Counting off heap overhead = 7% of 21GB = 3GB. So, actual --executor-memory = 21 - 3 = 18GB



  * */
}
