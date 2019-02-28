package org.peter.test.SparkSQLGuide;

import org.apache.spark.sql.*;

// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;

/**
 * @author peterpeng
 * @date 2019/2/28
 */
public class JavaSparkSQLExample {

	public static void main(String[] args) {
		//创建SparkSession，Spark程序的入口
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		runBasicDataFrameExample(spark);

	}

	private static void runBasicDataFrameExample(SparkSession spark) {
		Dataset<Row> df = spark.read().json("src/main/java/org/peter/test/SparkSQLGuide/people.json");

		//显示DataFrame的内容
		/**
		 * +----+-------+
		 * | age|   name|
		 * +----+-------+
		 * |null|Michael|
		 * |  30|   Andy|
		 * |  19| Justin|
		 * +----+-------+
		 */
		df.show();

		//打印表结构，以树的格式
		/**
		 * root
		 *  |-- age: long (nullable = true)
		 *  |-- name: string (nullable = true)
		 */
		df.printSchema();

		//选择name这一列
		/**
		 * +-------+
		 * |   name|
		 * +-------+
		 * |Michael|
		 * |   Andy|
		 * | Justin|
		 * +-------+
		 */
		df.select("name").show();

		//选择所有人，年龄加1
		/**
		 * +-------+---------+
		 * |   name|(age + 1)|
		 * +-------+---------+
		 * |Michael|     null|
		 * |   Andy|       31|
		 * | Justin|       20|
		 * +-------+---------+
		 */
		df.select(df.col("name"),df.col("age").plus(1)).show();
		df.select(functions.col("name"),functions.col("age").plus(1)).show();
		df.select(col("name"),col("age").plus(1)).show();

		//选择年龄大于21岁的人
		/**
		 * +---+----+
		 * |age|name|
		 * +---+----+
		 * | 30|Andy|
		 * +---+----+
		 */
		df.filter(col("age").gt(21)).show();

		//按年龄分组，统计个数
		/**
		 * +----+-----+
		 * | age|count|
		 * +----+-----+
		 * |  19|    1|
		 * |null|    1|
		 * |  30|    1|
		 * +----+-----+
		 */
		df.groupBy("age").count().show();

		//将DataFrame注册为SQL临时视图
		df.createOrReplaceTempView("people");

		//以编程方式运行sql
		/**
		 * +----+-------+
		 * | age|   name|
		 * +----+-------+
		 * |null|Michael|
		 * |  30|   Andy|
		 * |  19| Justin|
		 * +----+-------+
		 */
		Dataset<Row> sql = spark.sql("select * from people");
		sql.show();

		try {
			//创建全局临时视图
			df.createGlobalTempView("people");
			spark.sql("select * from global_temp.people").show();
		} catch (AnalysisException e) {
			e.printStackTrace();
		}

	}
}
