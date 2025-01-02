package com.project.hive;

import com.project.database.ProcessStatusRepository;
import com.project.utils.Status;
import com.project.utils.TimePartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;

import java.sql.Connection;
import java.util.List;

public class PartitionManager {
    private static final Logger logger = LogManager.getLogger(PartitionManager.class);
    private final SparkSession spark;
    private final String outputPath;
    private final String timezone;
    private final TimePartition partition;
    private final Connection conn;

    public PartitionManager(SparkSession spark, String outputPath, String timezone, TimePartition partition, Connection conn) {
        this.spark = spark;
        this.outputPath = outputPath;
        this.timezone = timezone;
        this.partition = partition;
        this.conn = conn;
    }

    /**
     * 파티션 데이터를 처리하고 저장합니다.
     *
     * @param inputPath   입력 데이터 경로
     * @param dates       처리할 날짜 목록
     * @param safeTimezone 타임존에 안전한 이름
     */
    public void processAndStorePartitions(String inputPath, List<String> dates, String safeTimezone) {
        for (String date : dates) {
            try {
                logger.info("데이터 처리 시작 - 날짜: {}", date);

                // 입력 데이터 읽기
                Dataset<Row> monthData = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(inputPath + date + ".csv");

                // 데이터 처리
                Dataset<Row> processedData = monthData
                        .withColumn("event_time_" + safeTimezone,
                                functions.from_utc_timestamp(
                                        functions.col("event_time"),
                                        timezone
                                )
                        )
                        .withColumn("partition_date",
                                functions.date_format(
                                        functions.col("event_time_" + safeTimezone),
                                        partition.getFormat()
                                )
                        );

                // 데이터 저장
                processedData.write()
                        .mode(SaveMode.Append)
                        .partitionBy("partition_date")
                        .parquet(outputPath + timezone + "/" + partition.getInputType());

                ProcessStatusRepository.create(conn, new com.project.entity.ProcessStatus(partition.getInputType(), date, Status.SUCCESS.getStatus(), "", timezone));

                logger.info("데이터 저장 완료 - 날짜: {}", date);

                // 메모리 해제
                monthData.unpersist();
                processedData.unpersist();
            } catch (Exception e) {
                logger.error("데이터 처리 중 오류 발생 - 날짜: {}", date, e);
            }
        }
    }
}
