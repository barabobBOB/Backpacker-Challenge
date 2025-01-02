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
import java.util.TimeZone;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        Map<String, Object> config = ConfigLoader.loadConfig("config.yml");

        if (config == null) {
            logger.error("설정 파일을 로드할 수 없습니다. 프로그램을 종료합니다.");
            return;
        }

        Map<String, Object> databaseConfig = (Map<String, Object>) config.get("database");
        Map<String, Object> appConfig = (Map<String, Object>) config.get("app");

        String url = (String) databaseConfig.get("url");
        String timezone = (String) appConfig.get("timezone");
        String startMonth = (String) appConfig.get("startMonth");
        String endMonth = (String) appConfig.get("endMonth");
        String inputPath = (String) appConfig.get("inputPath");
        String outputPath = (String) appConfig.get("outputPath");
        String partitionType = (String) appConfig.get("partition");
        TimePartition partition = TimePartition.valueOf(partitionType);
        TimeZone.setDefault(TimeZone.getTimeZone(timezone));

        Connection conn = DatabaseConnection.getConnection(url);
        ProcessStatusRepository.initialize(conn);

        SparkSession spark = SparkSession.builder()
                .appName("ECommerceActivityLog")
                .master("local[*]")
                .config("spark.driver.extraJavaOptions", "--add-opens java.base/java.net=ALL-UNNAMED")
                .config("spark.executor.extraJavaOptions", "--add-opens java.base/java.net=ALL-UNNAMED")
                .config("spark.sql.warehouse.dir", "./spark-warehouse")
                .enableHiveSupport()
                .getOrCreate();

        String tableName = "ecommerce_activity_log";
        String tableLocation = outputPath + timezone + "/" + partition;

        HiveTableManager hiveTableManager = new HiveTableManager(spark, tableName, tableLocation);

        List<String> date = Month.getMonthsInRange(startMonth, endMonth);
        String safeTimezone = timezone.replace("/", "_");

        PartitionManager partitionManager = new PartitionManager(spark, outputPath, timezone, partition, conn);
        partitionManager.processAndStorePartitions(inputPath, date, safeTimezone);

        hiveTableManager.createExternalTable();
        hiveTableManager.enableDynamicPartitioning();
        hiveTableManager.repairTablePartitions();
    }
}
