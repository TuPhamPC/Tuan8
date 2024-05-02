package com.java.part1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Part-1")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> data = spark.read()
				.option("inferSchema", true)
				.option("header", true)
				.csv("hdfs://localhost:9000/retails.csv");

		long cntCustomers = data.select("CustomerID").distinct().count() - 1; 

		long cntProdcts = data.select("StockCode").distinct().count();
		long cntInvoices = data.select("InvoiceNo").distinct().count();
		
		// print 
		System.out.println("Number of customer distinct: " + cntCustomers); 
		System.out.println("Number of product distinct: " + cntProdcts);
		System.out.println("Number of invoice distinct: " + cntInvoices);

	}
}
