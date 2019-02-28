# Spark SQL Guide

Spark SQL是用于结构化数据处理的Spark模块。用途是执行SQL查询，可用于从现有Hive安装中读取数据。
从其他编程语言中运行SQL时，结果作为Dataset和DataFrame返回。还可以使用JDBC与SQL接口进行交互。

Datasets和DataFrames
Dataset是分布式数据集合。可以从JVM对象被构造，然后使用功能性的转换。
DataFrame是一个组织成命名列的Dataset。它在概念上等同于关系数据库中的表。可以从多种来源构建，例如：
结构化数据文件，Hive中的表，外部数据库或现有RDD。
在Java中，DataFrame由Dataset<Row>表示。

## 入门

### 起点：SparkSession
Spark中所有功能的入口点都是SparkSession类。要创建基本的SparkSession，只需使用SparkSession.builder():

### 创建DataFrame
使用SparkSession,应用程序可以从现有的RDD，Hive表或Spark数据源创建DataFrame。

### DataFrame操作
除了简单的列引用和表达式之外，数据集还具有丰富的函数库。

### 以编程方式运行SQL查询
该sql上的功能SparkSession使应用程序以编程方式运行SQL查询并返回结果的Dataset<Row>。

### 全局临时视图
可以创建一个全局临时视图，在所有会话之间共享并保持活动状态，直到Spark应用程序终止。
全局临时视图与系统保留的数据库绑定global_temp。

### 创建Datasets
Datasets与RDDs类似，但是不使用Java序列化，而是使用专用的编码器来序列化对象以便通过网络进行处理或传输。
虽然编码器和标准序列化都负责将对象转换为字节，但编码器是动态生成的代码，并使用一种格式，允许Spark执行更多操作，而无须将字节反序列化为对象。

### 与RDD互操作

### 聚合

## 数据源

## 性能调优

## 分布式SQL引擎

## 使用Apache Arrow的Pandas PySpark使用指南

## 迁移指南

## 参考