package com.project.app;

import com.project.config.ConfigLoader;
import com.project.database.DatabaseConnection;
import com.project.database.ProcessStatusRepository;
import com.project.hive.HiveTableManager;
import com.project.hive.PartitionManager;
import com.project.utils.Month;
import com.project.utils.TimePartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        // src/main/resources/config.yml 설정파일 로딩
        Map<String, Object> config = ConfigLoader.loadConfig("config.yml");
        if (config == null) {
            logger.error("설정 파일을 로드할 수 없습니다. 프로그램을 종료합니다.");
            return;
        }

        // 설정 정보 추출
        Map<String, Object> databaseConfig = (Map<String, Object>) config.get("database");
        Map<String, Object> appConfig = (Map<String, Object>) config.get("app");

        String url = (String) databaseConfig.get("url");
        String timezone = (String) appConfig.get("timezone");
        String startMonth = (String) appConfig.get("startMonth");
        String endMonth = (String) appConfig.get("endMonth");
        String inputPath = (String) appConfig.get("inputPath");
        String outputPath = (String) appConfig.get("outputPath");
        String partitionType = (String) appConfig.get("partition");
        String derbyPath = (String) appConfig.get("derbyPath");
        TimePartition partition = TimePartition.valueOf(partitionType);
        Boolean retryFailed = (Boolean) appConfig.get("retryFailed");

        // 데이터베이스 연결 및 프로세스 상태 초기화
        Connection conn = DatabaseConnection.getConnection(url);
        ProcessStatusRepository.initialize(conn);

        // SparkSession 초기화
        SparkSession spark = initializeSparkSession();
        spark.conf().set("spark.sql.session.timeZone", "UTC"); // Spark 타임존을 UTC로 설정

        // 테이블 및 파티션 경로 설정
        String tableName = "ecommerce_activity_log";
        String tableLocation = outputPath + timezone + "/" + partition;

        HiveTableManager hiveTableManager = new HiveTableManager(spark, tableName, tableLocation, derbyPath);

        // 처리할 날짜 범위 계산
        List<String> date = Month.getMonthsInRange(startMonth, endMonth);
        String safeTimezone = timezone.replace("/", "_");

        PartitionManager partitionManager = new PartitionManager(spark, outputPath, timezone, partition, conn);

        // 실패한 작업 재시도 또는 새로운 데이터 처리
        if (retryFailed) {
            partitionManager.retryProcessAndStorePartitions(inputPath, date, safeTimezone);
        }
        if (!retryFailed) {
            partitionManager.processAndStorePartitions(inputPath, date, safeTimezone);
        }

        // Hive 테이블 관리 및 오류 복구
        try {
            hiveTableManager.manageHiveTable(safeTimezone, conn);
        } catch (RuntimeException e) {
            hiveTableManager.recoverBatch(hiveTableManager, safeTimezone, conn);
        }
    }
    /**
     * SparkSession을 초기화하고 설정합니다.
     *
     * @return 설정된 SparkSession 인스턴스
     */
    private static SparkSession initializeSparkSession() {
        return SparkSession.builder()
                .appName("ECommerceActivityLog") // 애플리케이션 이름
                .master("local[*]") // 로컬 모드 설정
                .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.net=ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions", "--add-opens java.base/java.net=ALL-UNNAMED")
                .config("spark.sql.warehouse.dir", "./spark-warehouse") // Spark의 기본 경로 설정
                .enableHiveSupport() // Hive 지원 활성화
                .getOrCreate();
    }
}
