package org.peter.test.Spark_JDBC_Impala;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * Spark
 * @author peterpeng
 * @date 2019/1/29
 */
public class JDBCConnectImpala {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
				.builder()
				.appName("Spark connect Impala by JDBC")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();

		// Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
		// Loading data from a JDBC source
		Dataset<Row> jdbcDF = spark.read()
				.format("jdbc")
				.option("driver","com.cloudera.impala.jdbc41.Driver")
				.option("url", "jdbc:impala://172.16.5.124:21050;AuthMech=3;UID=hdfs;PWD=hdfs")
				.option("dbtable", "peter_test.test_people3")
				.option("user", "hdfs")
				.option("password", "hdfs")
				.load();

		StructType schema = jdbcDF.schema();
		System.out.println(schema);
		//StructType(StructField(age,LongType,true), StructField(name,StringType,true), StructField(gender,StringType,true), StructField(country,StringType,true))

		jdbcDF.show();
		/**
		 * +---+-------+------+-------+
		 * |age|   name|gender|country|
		 * +---+-------+------+-------+
		 * | 10|Michael|  male|     CN|
		 * | 19| Justin|  male|     CN|
		 * | 30|   Andy|female|     US|
		 * +---+-------+------+-------+
		 */
	}
}
