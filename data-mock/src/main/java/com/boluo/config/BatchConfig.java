package com.boluo.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Optional;

/**
 * @author chao
 * @datetime 2025-01-06 22:19
 * @description
 */
@Slf4j
public class BatchConfig {

    @Getter
    @Setter
    private BatchYamlConfig yamlConfig;

    private static BatchConfig instance;
    private String jobType;
    private String env;

    private BatchConfig() {
    }

    public static BatchConfig getInstance() {
        if (instance == null) {
            instance = new BatchConfig();
        }
        return instance;
    }

    public BatchConfig load(String[] args) {

        // todo load jobs from config table
        String params = args[0];
        System.out.println("params: " + params);

        Gson gson = new GsonBuilder().serializeNulls().create();
        JsonObject jsonObject = gson.fromJson(params, JsonObject.class);
        env = jsonObject.get(Constants.JOB_ENV).getAsString().toLowerCase();
        jobType = jsonObject.get(Constants.JOB_TYPE).getAsString();

        Yaml yaml = new Yaml();
        InputStream inputStream = BatchConfig.class.getClassLoader().getResourceAsStream("application-" + env + ".yml");
        BatchYamlConfig yamlConfig = yaml.loadAs(inputStream, BatchYamlConfig.class);
        System.out.println("Load source and job table success ");

        // todo populate source info
        yamlConfig.getJobs().forEach(k -> {
            // Optional<BatchYamlConfig.Source> source = yamlConfig.getSources().stream().filter(m -> m.getName().equals(k.getSourceName())).findFirst();
            Optional<BatchYamlConfig.Source> source = Optional.empty();
            source.ifPresent(k::setSource);
        });

        setYamlConfig(yamlConfig);
        return this;
    }


    public String getJobType() {
        return jobType;
    }


}
