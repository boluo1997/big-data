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
    private List<Dest> dest;


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
    public static class Dest {
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
        private String sourceSchema;
        private String sourceTable;
        private String ingestEnabled;
        private String type;
        private String destTable;
        private String sqlQuery;
        private Source source;
        private Dest dest;
    }


    public List<Job> getDailyJobs() {
        return jobs.stream().filter(k -> k.getType().equalsIgnoreCase(Constants.DAILY)).filter(m -> Constants.YES.equalsIgnoreCase(m.getIngestEnabled())).collect(Collectors.toList());
    }

    public List<Job> getHourlyJobs() {
        return jobs.stream().filter(k -> k.getType().equalsIgnoreCase(Constants.HOURLY)).filter(m -> Constants.YES.equalsIgnoreCase(m.getIngestEnabled())).collect(Collectors.toList());
    }


}
