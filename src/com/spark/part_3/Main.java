package com.spark.part_3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Part-3").master("local").getOrCreate();
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("hdfs://localhost:9000/retails.csv");
		
		data.createOrReplaceTempView("data");
		spark.sql("select Country, sum(Quantity) as count from data group by Country order by count desc").show();
	}
}
