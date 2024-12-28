package com.project;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.TimeZone;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ECommerceActivityLog")
                .master("local[*]") // 로컬 모드에서 실행
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .enableHiveSupport()
                .getOrCreate();

        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Seoul"));
        System.out.println("SparkSession initialized successfully!");
    }
}
