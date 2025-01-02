package com.project.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

public class ConfigLoader {

    private static final Logger logger = LogManager.getLogger(ConfigLoader.class);

    /**
     * YAML 설정 파일을 로드합니다.
     *
     * @param configPath 설정 파일 경로 (resources 기준)
     * @return 로드된 설정 데이터를 포함하는 Map 객체
     */
    public static Map<String, Object> loadConfig(String configPath) {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(configPath)) {
            if (input == null) {
                logger.error("설정 파일을 찾을 수 없습니다: {}", configPath);
                return null;
            }
            Yaml yaml = new Yaml();
            Map<String, Object> config = yaml.load(input);
            logger.info("설정 파일 로드 완료: {}", configPath);
            return config;
        } catch (Exception e) {
            logger.error("설정 파일 로드 실패", e);
            return null;
        }
    }
}
