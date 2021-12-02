package com.spark.part_4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Part-4").master("local").getOrCreate();
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("hdfs://localhost:9000/retails.csv");
		
//		data.createOrReplaceTempView("data");
//		long cnt1 = data.count(); // 541909
		
		data.flatMap(new FlatMapFunction<Row, Row>() {
			private static final long serialVersionUID = 1L;
			private int cnt = 0;
			
			@Override
			public Iterator<Row> call(Row r) throws Exception {
				List<String> listItem = Arrays.asList(r.getString(2).split(" "));
				
				List<Row> listItemRow = new ArrayList<Row>();
				for (String item : listItem) {
					listItemRow.add(RowFactory.create(cnt, item, 1));
					cnt++;
				}
				
				return listItemRow.iterator();
			}
		}, RowEncoder.apply(new StructType().add("number", "integer").add("word", "string").add("lit", "integer"))).createOrReplaceTempView("data");
		
		spark.sql("select word, count(lit) from data group by word").show();
		
//		long cnt2 = spark.sql("select word from data").count();
//		System.out.println(cnt1 + " " + cnt2);
//		System.out.println(cnt1);
	}
}
