package org.peter.test.Spark_Parquest;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 写本地parquet文件，读本地parquet文件
 * @author peterpeng
 * @date 2019/1/18
 */
public class LoadingDataProgrammatically {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		Dataset<Row> peopleDF = spark.read().json("src/main/java/org/peter/test/Spark_Parquest/people.json");
		// DataFrames can be saved as Parquet files, maintaining the schema information
		// 如果文件存在，则会报：
		// Exception in thread "main" org.apache.spark.sql.AnalysisException: path file:/D:/code/diea/Sparklearn/people.parquet already exists.;
		//peopleDF.write().parquet("people.parquet");
		// 可以改为以添加模式写入
		peopleDF.write().mode("append").parquet("people.parquet");

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");

		// Parquet files can also be used to create a temporary view and then used in SQL statements
		parquetFileDF.createOrReplaceTempView("parquetFile");
		/*Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		namesDF.show();
		Dataset<String> namesDS = namesDF.map(
				(MapFunction<Row, String>) row -> "Name: " + row.getString(0),
				Encoders.STRING());
		namesDS.show();*/
		Dataset<Row> namesDF = spark.sql("SELECT name,age FROM parquetFile");
		namesDF.show();
		//result:
		/**
		 * +-------+----+
		 * |   name| age|
		 * +-------+----+
		 * |Michael|null|
		 * |   Andy|  30|
		 * | Justin|  19|
		 * +-------+----+
		 */
		spark.stop();
	}
}
