package com.boluo.config;

/**
 * @author chao
 * @datetime 2025-01-06 22:19
 * @description
 */
public class BatchConfig {

    private static BatchConfig batchConfig;

    public static BatchConfig getInstance() {
        if (batchConfig == null) {
            batchConfig = new BatchConfig();
        }
        return batchConfig;
    }

    public BatchConfig load(String[] args) {
        String params = args[0];
        return this;
    }

}
