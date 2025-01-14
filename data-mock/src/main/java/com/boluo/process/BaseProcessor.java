package com.boluo.process;

import com.boluo.config.BatchYamlConfig;
import com.boluo.utils.HttpUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;

import java.util.List;
import java.util.Map;

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

    protected long ingest(BatchYamlConfig.Job job, String batchId) {
        System.out.println("start ingest...");

        Map<String, String> params = HttpUtils.parseParams(job.getParams());
        Dataset<Row> sourceDs = SparkSession.active().emptyDataFrame();

        BatchYamlConfig.Dest destDB = job.getDest();
        SparkUtils.writeToMySQL(sourceDs, destDB.getJdbcUrl(), destDB.getDbName(), destDB.getUsername(), destDB.getPassword());
        return 0;
    }

}

