package com.project.hive;

import com.project.database.ProcessStatusRepository;
import com.project.entity.ProcessStatus;
import com.project.utils.Status;
import com.project.utils.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveTableManager {
    private static final Logger logger = LogManager.getLogger(HiveTableManager.class);
    private static final int MAX_RETRY_COUNT = 3;
    private final SparkSession spark;
    private final String tableName;
    private final String tableLocation;
    private final String derbyPath;

    public HiveTableManager(SparkSession spark, String tableName, String tableLocation, String derbyPath) {
        this.spark = spark;
        this.tableName = tableName;
        this.tableLocation = tableLocation;
        this.derbyPath = derbyPath;
    }

    /**
     * Hive 테이블이 존재하는지 확인합니다.
     *
     * @return 테이블이 존재하면 true, 존재하지 않으면 false
     */
    public boolean isTableExists() {
        String jdbcUrl = "jdbc:derby:" + derbyPath;
        String query = "SELECT TB.TBL_NAME " +
                "FROM TBLS TB " +
                "JOIN DBS DB ON TB.DB_ID = DB.DB_ID " +
                "WHERE DB.NAME = 'default' AND TB.TBL_NAME = '" + tableName + "'";

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                logger.info("Hive 테이블이 이미 존재합니다: {}", tableName);
                return true;
            }
        } catch (java.sql.SQLException e){
            if (e.getMessage().contains("not found")){
                logger.warn("데이터베이스가 존재하지 않습니다.");
            } else {
                logger.error("Hive 메타스토어에서 테이블 존재 여부 확인 중 오류 발생", e);
                throw new RuntimeException("테이블 존재 여부 확인 실패", e);
            }
        }
        catch (Exception e) {
            logger.error("Hive 메타스토어에서 테이블 존재 여부 확인 중 오류 발생", e);
        }

        logger.info("Hive 테이블이 존재하지 않습니다: {}", tableName);
        return false;
    }

    /**
     * Hive 테이블 생성.
     */
    public void createExternalTable(String partition) {
        if (isTableExists()) {
            logger.info("Hive 테이블 생성 생략: {}", tableName);
            return;
        }

        try {
            String createTableQuery = String.format(
                    "CREATE EXTERNAL TABLE IF NOT EXISTS %s (" +
                            "event_time TIMESTAMP, " +
                            "event_type STRING, " +
                            "product_id INT, " +
                            "category_id BIGINT, " +
                            "category_code STRING, " +
                            "brand STRING, " +
                            "price DOUBLE, " +
                            "user_id INT, " +
                            "user_session STRING, " +
                            "event_time_" + partition + " TIMESTAMP" +
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

    /**
     * Hive 작업 복구를 수행합니다.
     *
     * @param hiveTableManager HiveTableManager 객체
     * @param safeTimezone 안전한 타임존 값
     * @param conn 데이터베이스 연결 객체
     */
    public void recoverBatch(HiveTableManager hiveTableManager, String safeTimezone, Connection conn) {
        try {
            logger.info("Hive 작업 복구를 시작합니다.");
            recoverHiveOperations(hiveTableManager, safeTimezone, conn);
            repairHiveMetadata(hiveTableManager, conn);
            logger.info("Hive 작업 복구 완료.");
        } catch (Exception e) {
            logger.error("Hive 작업 복구 중 치명적 오류 발생: {}", e.getMessage());
            logFailure(conn, "HiveBatchRecovery", e.getMessage());
            sendAlert("Hive 복구 실패: " + e.getMessage());
        }
    }

    /**
     * 관리자에게 알림을 전송합니다.
     *
     * @param message 알림 메시지
     */
    private void sendAlert(String message) {
        // TODO: 이메일 또는 Slack 알림 전송
        logger.info("관리자에게 알림 전송: {}", message);
    }

    /**
     * Hive 테이블 관리 작업을 수행합니다.
     *
     * @param safeTimezone 안전한 타임존 값
     * @param conn 데이터베이스 연결 객체
     */
    public void manageHiveTable(String safeTimezone, Connection conn) {
        try {
            createExternalTable(safeTimezone);
            enableDynamicPartitioning();
            repairTablePartitions();
        } catch (Exception e) {
            logger.error("Hive 관련 오류 발생", e);
            ProcessStatusRepository.create(conn, new ProcessStatus(
                    "", "HiveOperation", Status.FAILURE.getStatus(), e.getMessage(), "", Type.HIVE.getType()
            ));
            throw new RuntimeException("Hive 작업 실패", e);
        }
    }

    /**
     * Hive 메타데이터를 복구합니다.
     *
     * @param hiveTableManager HiveTableManager 객체
     * @param conn 데이터베이스 연결 객체
     */

    private void repairHiveMetadata(HiveTableManager hiveTableManager, Connection conn) {
        try {
            logger.info("Hive 메타데이터 복구를 시작합니다.");
            hiveTableManager.repairTablePartitions();
            logger.info("Hive 메타데이터 복구 성공.");
        } catch (Exception e) {
            logger.error("Hive 메타데이터 복구 실패: {}", e.getMessage());
            ProcessStatusRepository.create(conn, new ProcessStatus(
                    "", "HiveMetadataRepair", Status.FAILURE.getStatus(),
                    e.getMessage(), "", Type.HIVE.getType()
            ));
        }
    }

    /**
     * Hive 작업을 복구합니다.
     *
     * @param hiveTableManager HiveTableManager 객체
     * @param safeTimezone 안전한 타임존 값
     * @param conn 데이터베이스 연결 객체
     */
    private void recoverHiveOperations(HiveTableManager hiveTableManager, String safeTimezone, Connection conn) {
        int retryCount = 0;

        while (retryCount < MAX_RETRY_COUNT) {
            try {
                logger.info("Hive 작업 복구 시도 중... (재시도 횟수: {}/{})", retryCount + 1, MAX_RETRY_COUNT);
                hiveTableManager.manageHiveTable(safeTimezone, conn);
                logger.info("Hive 작업 복구 성공!");
                return;
            } catch (Exception e) {
                retryCount++;
                logger.error("Hive 작업 복구 실패: {}", e.getMessage());
                if (retryCount >= MAX_RETRY_COUNT) {
                    logger.error("Hive 작업 복구 최대 시도 횟수를 초과했습니다.");
                    ProcessStatusRepository.create(conn, new ProcessStatus(
                            "", "HiveOperationRecovery", Status.FAILURE.getStatus(),
                            "최대 재시도 횟수 초과: " + e.getMessage(), "", Type.HIVE.getType()
                    ));
                }
            }
        }
    }

    /**
     * Hive 작업 실패를 기록합니다.
     *
     * @param conn 데이터베이스 연결 객체
     * @param operation 실패한 작업 이름
     * @param errorMessage 오류 메시지
     */
    private void logFailure(Connection conn, String operation, String errorMessage) {
        ProcessStatusRepository.create(conn, new ProcessStatus(
                "", operation, Status.FAILURE.getStatus(), errorMessage, "", Type.HIVE.getType()
        ));
    }
}
