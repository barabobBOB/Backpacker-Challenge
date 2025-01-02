package com.project.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;

public class DatabaseConnection {

    private static final Logger logger = LogManager.getLogger(DatabaseConnection.class);

    /**
     * 데이터베이스 연결을 생성합니다.
     *
     * @param url 데이터베이스 URL
     * @return Connection 객체 (연결이 성공적이면), null (연결 실패 시)
     */
    public static Connection getConnection(String url) {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
            if (conn != null) {
                logger.info("데이터베이스 연결이 완료되었습니다.");
            }
        } catch (Exception e) {
            logger.error("데이터베이스 연결 실패", e);
        }
        return conn;
    }
}