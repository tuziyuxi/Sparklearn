package org.peter.test.SparkSQLGuide;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

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

		//runBasicDataSourceExample(spark);
		//runBasicParquetExample(spark);
		//runJsonDatasetExample(spark);
		runJdbcDatasetExample(spark);
		spark.stop();
	}

	private static void runJdbcDatasetExample(SparkSession spark) {
		//可以通过load/save或jdbc方法实现JDBC加载和保存

		Dataset<Row> jdbdDF = spark.read()
				.format("jdbc")
				.option("url", "jdbc:postgresql:dbserver")
				.option("dbtable", "schema.tablename")
				.option("user", "username")
				.option("password", "password")
				.load();

		jdbdDF.write()
				.format("jdbc")
				.option("url", "jdbc:postgresql:dbserver")
				.option("dbtable", "schema.tablename")
				.option("user", "username")
				.option("password", "password")
				.save();

		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "username");
		connectionProperties.put("password", "password");
		Dataset<Row> jdbcDF2 = spark.read().jdbc("url", "dbtable", connectionProperties);

		jdbcDF2.write()
				.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);


	}

	private static void runJsonDatasetExample(SparkSession spark) {
		String path = "src/main/resources/people.json";
		Dataset<Row> peopleDF = spark.read().json(path);
		/**
		 root
		 |-- age: long (nullable = true)
		 |-- name: string (nullable = true)
		 */
		peopleDF.printSchema();

		peopleDF.createOrReplaceTempView("people" );
		Dataset<Row> namesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
		/**
		 +------+
		 |  name|
		 +------+
		 |Justin|
		 +------+
		 */
		namesDF.show();

		List<String> jsonData = Arrays.asList(
				"{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		Dataset<String> anotherPeopleDataset  = spark.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeopleDF = spark.read().json(anotherPeopleDataset);
		/**
		 +----------------+----+
		 |         address|name|
		 +----------------+----+
		 |[Columbus, Ohio]| Yin|
		 +----------------+----+
		 */
		anotherPeopleDF.show();

	}

	private static void runBasicParquetExample(SparkSession spark) {
		Dataset<Row> peopleDF = spark.read().json("src/main/resources/people.json");
		peopleDF.write().parquet("people.parquet");

		Dataset<Row> parquetFileDF = spark.read().parquet("people.parquet");
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19");
		Dataset<String> namesDS = namesDF.map(row -> "Name:" + row.getString(0), Encoders.STRING());
		/**
		 +----+-------+
		 | age|   name|
		 +----+-------+
		 |null|Michael|
		 |  30|   Andy|
		 |  19| Justin|
		 +----+-------+
		 */
		namesDS.show();
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
