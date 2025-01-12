package com.boluo.config;

import lombok.*;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author chao
 * @datetime 2025-01-09 21:55
 * @description
 */
@Getter
@Setter
@ToString
public class BatchYamlConfig {

    private Source source;
    private List<Job> jobs;


    @Data
    public static class Source {
        private String name;
        private String type;
        private String jdbcUrl;
        private String driverClass;
        private String dbName;
        private String username;
        private String password;
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @ToString
    public static class Job {
        private String name;
        private String sourceName;
        private String sourceApiKey;
        private String sourceApiUrl;
        private String requestType;
        private String params;
        private String destTable;
        private String ingestType;
        private String ingestEnabled;
    }


    public List<Job> getDailyJobs() {
        return jobs.stream().filter(k -> k.getIngestType().equalsIgnoreCase(Constants.DAILY)).filter(m -> Constants.YES.equalsIgnoreCase(m.getIngestEnabled())).collect(Collectors.toList());
    }

    public List<Job> getHourlyJobs() {
        return jobs.stream().filter(k -> k.getIngestType().equalsIgnoreCase(Constants.HOURLY)).filter(m -> Constants.YES.equalsIgnoreCase(m.getIngestEnabled())).collect(Collectors.toList());
    }


}
