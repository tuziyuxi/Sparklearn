Spark 学习项目

relative/path/to/img.jpg?raw=true "Title"

![](./picture/spark.png?raw=true "test")


## 一 第一个例子：idea运行Spark程序（SimpleApp.java）

[查看Spark官网入门例子](https://spark.apache.org/docs/latest/quick-start.html)

### 1 添加Maven依赖

```xml
<dependency> <!-- Spark dependency -->
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-sql_2.11</artifactId>
  <version>2.4.0</version>
</dependency>
```

### 2 编写代码SimpleApp.java

### 3 配置vm options
-Dspark.master=local

https://github.com/tuziyuxi/Sparklearn/raw/master/picture/vmoptions.png

![-Dspark.master=local](https://github.com/tuziyuxi/Sparklearn/master/picture/vmoptions.png)

### 4 运行

会报一个Hadoop路径下找不到winutils.exe的错误，是window里的bug，暂时忽略。
然后结果正确。

```
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
19/01/17 20:50:16 INFO SparkContext: Running Spark version 2.4.0
19/01/17 20:50:16 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19/01/17 20:50:17 ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
	at org.apache.hadoop.util.Shell.getQualifiedBinPath(Shell.java:378)
	at org.apache.hadoop.util.Shell.getWinUtilsPath(Shell.java:393)
	at org.apache.hadoop.util.Shell.<clinit>(Shell.java:386)
	at org.apache.hadoop.util.StringUtils.<clinit>(StringUtils.java:79)
	at org.apache.hadoop.security.Groups.parseStaticMapping(Groups.java:116)
	at org.apache.hadoop.security.Groups.<init>(Groups.java:93)
	at org.apache.hadoop.security.Groups.<init>(Groups.java:73)
	at org.apache.hadoop.security.Groups.getUserToGroupsMappingService(Groups.java:293)
	at org.apache.hadoop.security.UserGroupInformation.initialize(UserGroupInformation.java:283)
	at org.apache.hadoop.security.UserGroupInformation.ensureInitialized(UserGroupInformation.java:260)
	at org.apache.hadoop.security.UserGroupInformation.loginUserFromSubject(UserGroupInformation.java:789)
	at org.apache.hadoop.security.UserGroupInformation.getLoginUser(UserGroupInformation.java:774)
	at org.apache.hadoop.security.UserGroupInformation.getCurrentUser(UserGroupInformation.java:647)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2422)
	at org.apache.spark.util.Utils$$anonfun$getCurrentUserName$1.apply(Utils.scala:2422)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.util.Utils$.getCurrentUserName(Utils.scala:2422)
	at org.apache.spark.SparkContext.<init>(SparkContext.scala:293)
	at org.apache.spark.SparkContext$.getOrCreate(SparkContext.scala:2520)
	at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:935)
	at org.apache.spark.sql.SparkSession$Builder$$anonfun$7.apply(SparkSession.scala:926)
	at scala.Option.getOrElse(Option.scala:121)
	at org.apache.spark.sql.SparkSession$Builder.getOrCreate(SparkSession.scala:926)
	at org.peter.test.SimpleApp.main(SimpleApp.java:14)
19/01/17 20:50:17 INFO SparkContext: Submitted application: Simple Application
...
19/01/17 20:50:34 INFO DAGScheduler: Job 1 finished: count at SimpleApp.java:18, took 0.151058 s
Lines with a: 2, lines with b: 1
19/01/17 20:50:34 INFO SparkUI: Stopped Spark web UI at http://XZ-161:4040
19/01/17 20:50:34 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/01/17 20:50:34 INFO MemoryStore: MemoryStore cleared
19/01/17 20:50:34 INFO BlockManager: BlockManager stopped
19/01/17 20:50:34 INFO BlockManagerMaster: BlockManagerMaster stopped
19/01/17 20:50:34 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
19/01/17 20:50:34 INFO SparkContext: Successfully stopped SparkContext
19/01/17 20:50:34 INFO ShutdownHookManager: Shutdown hook called
19/01/17 20:50:34 INFO ShutdownHookManager: Deleting directory C:\Users\Efun\AppData\Local\Temp\spark-e3a229cd-7483-4a89-9313-9f1cb4293d06
```

完。


