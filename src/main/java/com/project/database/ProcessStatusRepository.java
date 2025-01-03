package com.project.database;

import com.project.entity.ProcessStatus;
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
                "partition_key TEXT, " +
                "process_name TEXT NOT NULL, " +
                "status TEXT NOT NULL, " +
                "error_message TEXT, " +
                "last_updated DATETIME DEFAULT CURRENT_TIMESTAMP, " +
                "timezone TEXT, " +
                "type TEXT" +
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
    public static void create(Connection conn, ProcessStatus processStatus) {
        String insertSQL = "" +
                "INSERT INTO process_status (" +
                "partition_key, " +
                "process_name, " +
                "status, " +
                "error_message, " +
                "timezone," +
                "type" +
                ") " +
                "VALUES (?, ?, ?, ?, ?, ?);";

        try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
            pstmt.setString(1, processStatus.getPartitionKey());
            pstmt.setString(2, processStatus.getProcessName());
            pstmt.setString(3, processStatus.getStatus());
            pstmt.setString(4, processStatus.getErrorMessage());
            pstmt.setString(5, processStatus.getTimezone());
            pstmt.setString(6, processStatus.getType());

            pstmt.executeUpdate();

            logger.info("데이터 삽입 성공: " + processStatus.getProcessName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 주어진 조건에 해당하는 레코드의 ID를 반환합니다.
     *
     * @param conn 데이터베이스 연결 객체.
     * @param partitionKey 확인할 레코드의 파티션 키.
     * @param processName 확인할 레코드의 파일 이름.
     * @param timezone 확인할 레코드의 타임존.
     * @param status 확인할 상태 값.
     * @return 레코드의 ID (존재하지 않을 경우 -1 반환).
     */
    public static int getRecordId(Connection conn, String partitionKey, String processName, String timezone, String status) {
        String querySQL = "" +
                "SELECT id " +
                "FROM process_status " +
                "WHERE partition_key = ? " +
                "AND process_name = ? " +
                "AND timezone = ? " +
                "AND status = ?;";

        try (PreparedStatement pstmt = conn.prepareStatement(querySQL)) {
            pstmt.setString(1, partitionKey);
            pstmt.setString(2, processName);
            pstmt.setString(3, timezone);
            pstmt.setString(4, status);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id"); // ID 값 반환
                }
            }
        } catch (Exception e) {
            logger.error("데이터 ID 확인 중 오류 발생 - Partition Key: {}, File Name: {}, Timezone: {}", partitionKey, processName, timezone, e);
        }

        return -1; // 레코드가 없으면 -1 반환
    }
    /**
     * process_status 테이블에서 주어진 ID(PK)를 기준으로 레코드를 업데이트합니다.
     *
     * @param conn 데이터베이스 연결 객체.
     * @param id 업데이트할 레코드의 ID (PK).
     * @param processStatus 업데이트할 정보를 포함하는 ProcessStatus 객체.
     */
    public static void updateById(Connection conn, int id, ProcessStatus processStatus) {
        String updateSQL = "" +
                "UPDATE process_status " +
                "SET partition_key = ?, " +
                "    process_name = ?, " +
                "    status = ?, " +
                "    error_message = ?, " +
                "    timezone = ?, " +
                "    type = ? " +
                "WHERE id = ?;";

        try (PreparedStatement pstmt = conn.prepareStatement(updateSQL)) {
            pstmt.setString(1, processStatus.getPartitionKey());
            pstmt.setString(2, processStatus.getProcessName());
            pstmt.setString(3, processStatus.getStatus());
            pstmt.setString(4, processStatus.getErrorMessage());
            pstmt.setString(5, processStatus.getTimezone());
            pstmt.setString(6, processStatus.getType());
            pstmt.setInt(7, id);

            int rowsAffected = pstmt.executeUpdate();

            if (rowsAffected > 0) {
                logger.info("데이터 업데이트 성공: ID = " + id);
            } else {
                logger.warn("업데이트 대상 레코드가 없습니다: ID = " + id);
            }
        } catch (Exception e) {
            logger.error("데이터 업데이트 중 오류 발생 - ID: " + id, e);
        }
    }
}
