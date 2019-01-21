package org.peter.test.Spark_Parquest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 使用分区
 * @author peterpeng
 * @date 2019/1/19
 */
public class UsePartition {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		Dataset<Row> peopleDF = spark.read().json("src/main/java/org/peter/test/Spark_Parquest/people2.json");

		peopleDF.write().mode("append").partitionBy("gender","country").parquet("test_people");

		Dataset<Row> parquetFileDF = spark.read().parquet("test_people");

		parquetFileDF.createOrReplaceTempView("parquetFile");

		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile where gender = 'female'");
		namesDF.show();

		spark.stop();
	}
}
