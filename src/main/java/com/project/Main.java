package com.project;

import org.apache.spark.sql.*;

import java.util.TimeZone;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ECommerceActivityLog")
                .master("local[*]")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .enableHiveSupport()
                .getOrCreate();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));

        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("./src/main/java/com/project/data/input/2019-*.csv");

        Dataset<Row> processedData = rawData
                .withColumn("event_time_kst", functions.from_utc_timestamp(functions.col("event_time"), "Asia/Seoul"))
                .withColumn("partition_date", functions.date_format(functions.col("event_time_kst"), "yyyy-MM-dd"));

        processedData.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("partition_date")
                .parquet("./src/main/java/com/project/data/output/parquet/");

    }
}
