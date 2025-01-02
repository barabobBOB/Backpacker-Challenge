package com.project.hive;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class HiveTableManager {
    private static final Logger logger = LogManager.getLogger(HiveTableManager.class);
    private final SparkSession spark;
    private final String tableName;
    private final String tableLocation;

    public HiveTableManager(SparkSession spark, String tableName, String tableLocation) {
        this.spark = spark;
        this.tableName = tableName;
        this.tableLocation = tableLocation;
    }

    /**
     * Hive 테이블 생성.
     */
    public void createExternalTable() {
        try {
            String createTableQuery = String.format(
                    "CREATE EXTERNAL TABLE IF NOT EXISTS %s (" +
                            "user_id STRING, " +
                            "item_id STRING, " +
                            "category_id STRING, " +
                            "behavior STRING, " +
                            "event_time STRING" +
                            ") " +
                            "PARTITIONED BY (partition_date STRING) " +
                            "STORED AS PARQUET " +
                            "LOCATION '%s';",
                    tableName, tableLocation
            );

            spark.sql(createTableQuery);
            logger.info("Hive External Table 생성 완료: {}", tableName);
        } catch (Exception e) {
            logger.error("Hive External Table 생성 실패: {}", tableName, e);
        }
    }

    /**
     * 테이블 파티션 갱신.
     */
    public void repairTablePartitions() {
        try {
            String repairQuery = String.format("MSCK REPAIR TABLE %s", tableName);
            spark.sql(repairQuery);
            logger.info("테이블 파티션 갱신 완료: {}", tableName);
        } catch (Exception e) {
            logger.error("테이블 파티션 갱신 실패: {}", tableName, e);
        }
    }

    /**
     * 동적 파티션 설정.
     */
    public void enableDynamicPartitioning() {
        try {
            spark.sql("SET hive.exec.dynamic.partition = true");
            spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
            logger.info("동적 파티션 설정 완료");
        } catch (Exception e) {
            logger.error("동적 파티션 설정 실패", e);
        }
    }

    /**
     * 특정 파티션 수동 추가 (선택 사항).
     */
    public void addPartition(String partitionDate) {
        try {
            String addPartitionQuery = String.format(
                    "ALTER TABLE %s ADD PARTITION (partition_date='%s')",
                    tableName, partitionDate
            );
            spark.sql(addPartitionQuery);
            logger.info("파티션 추가 완료: {} - {}", tableName, partitionDate);
        } catch (Exception e) {
            logger.error("파티션 추가 실패: {} - {}", tableName, partitionDate, e);
        }
    }
}
