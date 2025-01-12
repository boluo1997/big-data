package com.boluo.process;

import com.boluo.config.BatchYamlConfig;
import com.boluo.utils.HttpUtils;

import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        HashMap<String, String> params = HttpUtils.parseParams(job.getParams());

        return 0L;
    }

}

