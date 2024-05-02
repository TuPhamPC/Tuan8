package com.java.part2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Part-2").master("local").getOrCreate();
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("hdfs://localhost:9000/retails.csv");
		long cntCustomers = data.select("CustomerID").count();
		long cntCustomersNoInfor = data.select("CustomerID").filter(data.col("CustomerID").isNull()).count();
		
		double ratio = (double) cntCustomersNoInfor / cntCustomers * 100;
		System.out.printf("Ratio no information: %f \n", ratio);
	}
}
