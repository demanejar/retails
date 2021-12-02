package com.spark.part_5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Part-4").master("local").getOrCreate();
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("hdfs://localhost:9000/retails.csv");
		
		data.filter(data.col("Country").equalTo("United Kingdom")).createOrReplaceTempView("data");
		spark.sql("select Description, sum(Quantity) as count from data group by Description order by count desc").show();
	}
}
