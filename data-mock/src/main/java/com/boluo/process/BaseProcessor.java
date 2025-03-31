package com.boluo.process;

import com.boluo.config.BatchYamlConfig;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utils.DatasetUtils;
import utils.SparkUtils;

import javax.ws.rs.HttpMethod;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

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
        Dataset<Row> sourceDs = getApiInfo(job, batchId);

        BatchYamlConfig.Dest destDB = job.getDest();
        DatasetUtils.writeToMySQL(sourceDs, destDB.getJdbcUrl(), job.getDestTable(), destDB.getUsername(), destDB.getPassword());
        return 0;
    }

    private Dataset<Row> getApiInfo(BatchYamlConfig.Job job, String batchId) {
        // Map<String, String> params = HttpUtils.parseParams(job.getParams());
        String apiUrl = job.getSourceApiUrl();
        String apiKey = job.getSourceApiKey();
        String date = timeFormatFunc(LocalDate.now());
        HashMap<String, String> params = new HashMap<String, String>() {{
            put("key", apiKey);
            put("date", date);
        }};

        String url = String.format("%s?%s", apiUrl, utils.HttpUtils.params(params));
        JsonNode responseNode = utils.HttpUtils.apiRequest(HttpMethod.GET, url, null, null);
        // System.out.println("response body: \n" + responseNode.toString());

        Dataset<Row> ds = SparkSession.active().sql("select 1")
                .withColumn("raw_msg", expr(String.format("'%s'", responseNode)))
                .withColumn("batch_id", expr(String.format("'%s'", batchId)))
                .withColumn("load_date", current_date())
                .drop("1");
        ds.show(false);
        return ds;
    }

    public static String timeFormatFunc(LocalDate date) {
        // 日期规则, 格式:月/日 如:1/1,/10/1,12/12 如月或者日小于10,前面无需加0
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d");
        return date.format(formatter);
    }

}

