package com.boluo.process;

import com.boluo.config.BatchConfig;
import com.boluo.config.BatchYamlConfig;
import org.apache.spark.sql.SparkSession;

import java.util.List;


/**
 * @author chao
 * @datetime 2025-01-09 22:23
 * @description
 */
public class HourlyProcessor extends BaseProcessor {

    private final SparkSession sparkSession;
    private final BatchConfig batchConfig;

    public HourlyProcessor(SparkSession sparkSession, BatchConfig batchConfig) {
        this.sparkSession = sparkSession;
        this.batchConfig = batchConfig;
    }

    @Override
    public List<? extends BatchYamlConfig.Job> getJobs() {
        return batchConfig.getYamlConfig().getHourlyJobs();
    }

    @Override
    public void executeTasks(List<BatchYamlConfig.Job> jobList) {
        System.out.println("execute hourly process...");
    }


}

