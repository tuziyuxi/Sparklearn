package org.peter.test.SparkSQLGuide;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author peterpeng
 * @date 2019/3/1
 */
public class JavaSQLDataSourceExample {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL data sources example")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		runBasicDataSourceExample(spark);

		spark.stop();
	}

	private static void runBasicDataSourceExample(SparkSession spark) {
		//不需要指定数据源类型
		Dataset<Row> usersDF = spark.read().load("src/main/java/org/peter/test/SparkSQLGuide/users.parquet");
		usersDF.select("name","favorite_color").write().mode("ignore").save("namesAndFavColors.parquet");

		//加载JSON文件
		Dataset<Row> peopleDF = spark.read().format("json").load("src/main/java/org/peter/test/SparkSQLGuide/people.json");
		peopleDF.select("name","age").write().mode("ignore").format("parquet").save("namesAndAges.parquet");

		//加载csv文件
		Dataset<Row> peopleDFCsv = spark.read().format("csv")
				.option("sep", ";")
				.option("inferSchema", "true")
				.option("header", "true")
				.load("src/main/java/org/peter/test/SparkSQLGuide/people.csv");

		//写文件，也可以带选项配置
		usersDF.write().mode("ignore").format("orc")
				.option("orc.bloom.filter.columns", "favorite_color")
				.option("orc.dictionary.key.threshold", "1.0")
				.save("users_with_options.orc");

		//sql直接在文件上运行sql
		/**
		 * +------+--------------+----------------+
		 * |  name|favorite_color|favorite_numbers|
		 * +------+--------------+----------------+
		 * |Alyssa|          null|  [3, 9, 15, 20]|
		 * |   Ben|           red|              []|
		 * +------+--------------+----------------+
		 */
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`");
		sqlDF.show();

		//saveAsTable保存到持久表
		peopleDF.write().bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed");

		//parquet文件分区
		usersDF.write().partitionBy("favorite_color").format("parquet").mode("ignore").save("namesPartByColor.parquet");

		//bucketBy 和 partitionBy 一起使用
		peopleDF.write().partitionBy("age").bucketBy(42,"name").saveAsTable("people_partitioned_bucketed");

	}
}
