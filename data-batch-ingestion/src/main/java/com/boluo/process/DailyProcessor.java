package com.boluo.process;

import com.boluo.config.BatchConfig;
import com.boluo.config.BatchYamlConfig;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author chao
 * @datetime 2025-01-09 22:23
 * @description
 */
public class DailyProcessor extends BaseProcessor {

    private final SparkSession sparkSession;
    private final BatchConfig batchConfig;

    public DailyProcessor(SparkSession sparkSession, BatchConfig batchConfig) {
        this.sparkSession = sparkSession;
        this.batchConfig = batchConfig;
    }

    @Override
    public List<? extends BatchYamlConfig.Job> getJobs() {
        return batchConfig.getYamlConfig().getDailyJobs();
    }

    @Override
    public void executeTasks(List<BatchYamlConfig.Job> jobList) {
        System.out.println("execute new daily process...");
        ThreadPoolExecutor service = new ThreadPoolExecutor(50,
                100,
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(50),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy()
        );

        String batchId = UUID.randomUUID().toString();
        for (BatchYamlConfig.Job job : jobList) {
            service.execute(() -> {
                ingest(job, batchId);
            });
        }
        service.shutdown();
    }


}

