package org.peter.test.Spark_Parquest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 读写HDFS上的parquet文件
 * 就是把路径改为HDFS上的路径就可以了
 * @author peterpeng
 * @date 2019/1/19
 */
public class HDFSParquet {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		Dataset<Row> peopleDF = spark.read().json("src/main/java/org/peter/test/Spark_Parquest/people2.json");

		String parquetUrl = "hdfs://172.16.5.127:8020/petertest/test_people";

		peopleDF.write().mode("append").partitionBy("gender","country")
				.parquet(parquetUrl);

		Dataset<Row> parquetFileDF = spark.read().parquet(parquetUrl);

		parquetFileDF.createOrReplaceTempView("parquetFile");

		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile where gender = 'female'");
		namesDF.show();

		/**
		 * result:
		 *
		 * +----+
		 * |name|
		 * +----+
		 * |Andy|
		 * |Andy|
		 * +----+
		 */
		spark.stop();
	}
}
