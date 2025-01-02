package com.project.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;


/**
 * 데이터베이스에서 프로세스 상태 레코드를 관리하는 레포지토리 클래스.
 * 테이블 초기화, 데이터 삽입, 존재 여부 확인 기능을 처리합니다.
 */
public class ProcessStatusRepository {

    private static final Logger logger = LogManager.getLogger(ProcessStatusRepository.class);

    /**
     * 데이터베이스에 process_status 테이블을 초기화합니다.
     * 테이블이 존재하지 않는 경우 생성됩니다.
     *
     * @param conn 데이터베이스 연결 객체.
     */
    public static void initialize(Connection conn) {
        String createTableSQL = "" +
                "CREATE TABLE IF NOT EXISTS process_status (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                "partition_key TEXT NOT NULL, " + // 쉼표 추가
                "file_name TEXT NOT NULL, " +
                "status TEXT NOT NULL, " +
                "error_message TEXT, " +
                "last_updated DATETIME DEFAULT CURRENT_TIMESTAMP, " +
                "timezone TEXT" +
                ");";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);

            logger.info("성공적으로 데이터베이스 테이블 초기화를 완료했습니다.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * process_status 테이블에 새로운 레코드를 삽입합니다.
     *
     * @param conn 데이터베이스 연결 객체.
     * @param processStatus 삽입할 레코드의 정보를 포함하는 ProcessStatus 객체.
     */
    public static void create(Connection conn, com.project.entity.ProcessStatus processStatus) {
        String insertSQL = "" +
                "INSERT INTO process_status (" +
                "partition_key, " +
                "file_name, " +
                "status, " +
                "error_message, " +
                "timezone" +
                ") " +
                "VALUES (?, ?, ?, ?, ?);";

        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setString(1, processStatus.getPartitionKey());
            pstmt.setString(2, processStatus.getFileName());
            pstmt.setString(3, processStatus.getStatus());
            pstmt.setString(4, processStatus.getErrorMessage());
            pstmt.setString(5, processStatus.getTimezone());

            pstmt.executeUpdate();

            logger.info("데이터 삽입 성공: " + processStatus.getFileName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 주어진 조건에 해당하는 레코드가 process_status 테이블에 존재하는지 확인합니다.
     *
     * @param conn 데이터베이스 연결 객체.
     * @param partitionKey 확인할 레코드의 파티션 키.
     * @param fileName 확인할 레코드의 파일 이름.
     * @param timezone 확인할 레코드의 타임존.
     * @return 레코드가 존재하면 true, 그렇지 않으면 false.
     */
    public static boolean exists(Connection conn, String partitionKey, String fileName, String timezone) {
        String querySQL = "SELECT COUNT(*) FROM process_status WHERE partition_key = ? AND file_name = ? AND timezone = ?;";

        try (PreparedStatement pstmt = conn.prepareStatement(querySQL)) {
            pstmt.setString(1, partitionKey);
            pstmt.setString(2, fileName);
            pstmt.setString(3, timezone);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (Exception e) {
            logger.error("데이터 존재 여부 확인 중 오류 발생 - Partition Key: {}, File Name: {}, Timezone: {}", partitionKey, fileName, timezone, e);
        }

        return false;
    }
}
