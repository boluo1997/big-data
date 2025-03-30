package com.boluo.process;

import com.boluo.config.BatchYamlConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import utils.DatasetUtils;
import utils.SparkUtils;

import java.util.List;

/**
 * @author chao
 * @datetime 2025-01-09 22:17
 * @description
 */
public abstract class BaseProcessor implements Processor {

    public void process() {
        List<BatchYamlConfig.Job> jobs = (List<BatchYamlConfig.Job>) getJobs();
        System.out.println("current jobs: " + jobs.toString());
        executeTasks(jobs);
    }

    public abstract List<? extends BatchYamlConfig.Job> getJobs();

    public abstract void executeTasks(List<BatchYamlConfig.Job> jobList);

    protected long ingest(BatchYamlConfig.Job job, String batchID) {
        // TODO
        System.out.println("test ...");
        BatchYamlConfig.Source sourceDB = job.getSource();
        Dataset<Row> sourceDs = DatasetUtils.readFromMySQL(sourceDB.getJdbcUrl(), job.getSourceTableName(), sourceDB.getUsername(), sourceDB.getPassword());
        sourceDs.show(false);
        return 0L;
    }

}

