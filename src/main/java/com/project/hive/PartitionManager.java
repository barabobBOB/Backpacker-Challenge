package com.project.hive;

import com.project.database.ProcessStatusRepository;
import com.project.entity.ProcessStatus;
import com.project.utils.Status;
import com.project.utils.TimePartition;
import com.project.utils.Type;
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

                if (ProcessStatusRepository.getRecordId(conn, partition.getInputType(), date, timezone, Status.SUCCESS.getStatus()) != -1) {
                    logger.info("이미 처리된 데이터 - 날짜: {}", date);
                    continue;
                }

                logger.info("데이터 처리 시작 - 날짜: {}", date);

                // 입력 데이터 읽기
                Dataset<Row> monthData = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(inputPath + date + ".csv");

                // 데이터 처리
                Dataset<Row> processedData = monthData
                        .withColumn("event_time", functions.col("event_time").cast("timestamp"))
                        .withColumn("partition_date",
                                functions.date_format(
                                        functions.from_utc_timestamp(functions.col("event_time"), timezone),
                                        partition.getFormat()
                                )
                        );

                // 데이터 저장
                processedData.write()
                        .mode(SaveMode.Append)
                        .partitionBy("partition_date")
                        .parquet(outputPath + timezone + "/" + partition.getInputType());

                ProcessStatusRepository.create(conn, new ProcessStatus(partition.getInputType(), date, Status.SUCCESS.getStatus(), "", timezone, Type.FILE.getType()));

                logger.info("데이터 저장 완료 - 날짜: {}", date);

                // 메모리 해제
                monthData.unpersist();
                processedData.unpersist();
            } catch (Exception e) {
                logger.error("데이터 처리 중 오류 발생 - 날짜: {}", date, e);

                ProcessStatusRepository.create(conn, new ProcessStatus(
                        partition.getInputType(), date, Status.FAILURE.getStatus(), e.getMessage(), timezone, Type.FILE.getType()
                ));
            }
        }
    }

    /**
     * 지정된 날짜 리스트에 대해 데이터를 처리하고, 처리되지 않은 데이터를 파티셔닝하여 저장합니다.
     *
     * @param inputPath   입력 데이터의 경로 (CSV 파일 경로)
     * @param dates       처리할 날짜 리스트 (yyyy-MM 형식)
     * @param safeTimezone 타임존 변환에 사용할 안전한 타임존 값
     */
    public void retryProcessAndStorePartitions(String inputPath, List<String> dates, String safeTimezone) {
        int processStatusID = -1;

        for (String date : dates) {
            try {
                processStatusID = ProcessStatusRepository.getRecordId(conn, partition.getInputType(), date, timezone, Status.SUCCESS.getStatus());

                if (processStatusID != -1) {
                    logger.info("이미 처리된 데이터 - 날짜: {}", date);
                    continue;
                }

                logger.info("데이터 처리 시작 - 날짜: {}", date);

                // 입력 데이터 읽기 (월 전체 데이터)
                Dataset<Row> monthData = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .csv(inputPath + date + ".csv");

                // 데이터 처리 및 파티셔닝
                Dataset<Row> processedData = monthData
                        .withColumn("event_time_" + safeTimezone,
                                functions.from_utc_timestamp(functions.col("event_time"), timezone))
                        .withColumn("partition_date",
                                functions.date_format(functions.col("event_time_" + safeTimezone), "yyyy-MM-dd"));

                // 날짜별로 데이터 저장 처리
                List<String> distinctDates = processedData
                        .select("partition_date")
                        .distinct()
                        .as(Encoders.STRING())
                        .collectAsList();

                String partitionPath = outputPath + timezone + "/" + partition.getInputType();

                for (String partitionDate : distinctDates) {
                    Dataset<Row> partitionData = processedData.filter(
                            functions.col("partition_date").equalTo(partitionDate)
                    );
                    processPartition(partitionDate, partitionData, partitionPath);
                }

                // 성공 상태 저장
                ProcessStatusRepository.updateById(conn, processStatusID, new ProcessStatus(
                        partition.getInputType(), date, Status.SUCCESS.getStatus(), "", timezone, Type.FILE.getType()
                ));

                logger.info("데이터 저장 완료 - 날짜: {}", date);

                // 메모리 해제
                monthData.unpersist();
                processedData.unpersist();
            } catch (Exception e) {
                logger.error("데이터 처리 중 오류 발생 - 날짜: {}", date, e);

                // 실패 상태 저장
                ProcessStatusRepository.updateById(conn, processStatusID, new ProcessStatus(
                        partition.getInputType(), date, Status.FAILURE.getStatus(), e.getMessage(), timezone, Type.FILE.getType()
                ));
            }
        }
    }

    /**
     * 주어진 날짜와 데이터셋에 대해 파티션 존재 여부를 확인하고 데이터를 저장하거나 병합합니다.
     *
     * @param partitionDate 파티션 날짜
     * @param processedData 처리된 데이터셋
     * @param partitionPath 파티션 저장 경로
     */
    private void processPartition(String partitionDate, Dataset<Row> processedData, String partitionPath) {
        String fullPartitionPath = partitionPath + "/partition_date=" + partitionDate;

        try {
            if (partitionExists(partitionPath, partitionDate)) {
                // 기존 데이터 읽기
                Dataset<Row> existingData = spark.read().parquet(fullPartitionPath);

                // 병합 및 중복 제거
                Dataset<Row> mergedData = processedData
                        .union(existingData)
                        .dropDuplicates("event_time", "user_id", "product_id", "event_type", "user_session");

                // 병합된 데이터를 덮어쓰기
                mergedData.write()
                        .mode(SaveMode.Overwrite)
                        .partitionBy("partition_date")
                        .parquet(partitionPath);

                logger.info("파티션 병합 완료 - 날짜: {}", partitionDate);
            } else {
                // 파티션이 없으면 바로 저장
                processedData.write()
                        .mode(SaveMode.Append)
                        .partitionBy("partition_date")
                        .parquet(partitionPath);

                logger.info("새 파티션 저장 완료 - 날짜: {}", partitionDate);
            }
        } catch (Exception e) {
            logger.error("파티션 처리 중 오류 발생 - 날짜: {}", partitionDate, e);
            throw new RuntimeException("파티션 처리 실패", e);
        }
    }

    /**
     * 주어진 파티션이 존재하는지 확인합니다.
     *
     * @param partitionPath 파티션 저장 경로
     * @param partitionDate 확인할 파티션 날짜
     * @return 파티션이 존재하면 true, 아니면 false
     */
    private boolean partitionExists(String partitionPath, String partitionDate) {
        String fullPartitionPath = partitionPath + "/partition_date=" + partitionDate;

        try {
            Dataset<Row> existingData = spark.read().parquet(fullPartitionPath);
            return !existingData.isEmpty();
        } catch (Exception e) {
            logger.info("파티션이 존재하지 않습니다 - 경로: {}", fullPartitionPath);
            return false;
        }
    }
}
