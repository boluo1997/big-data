package com.boluo.process;

import com.boluo.config.BatchConfig;
import com.boluo.config.Constants;
import org.apache.spark.sql.SparkSession;
import utils.SparkUtils;


/**
 * @author chao
 * @datetime 2025-01-09 22:16
 * @description
 */

public class ProcessorFactory {

    private static ProcessorFactory instance;
    private final SparkSession sparkSession = SparkUtils.initialSpark();

    public static ProcessorFactory getInstance() {
        if (instance == null) {
            instance = new ProcessorFactory();
        }
        return instance;
    }

    public Processor getProcessor(BatchConfig batchConfig) {
        String jobType = batchConfig.getJobType();
        if (Constants.DAILY.equals(jobType)) {
            return new DailyProcessor(sparkSession, batchConfig);
        } else if (Constants.HOURLY.equals(jobType)) {
            return new HourlyProcessor(sparkSession, batchConfig);
        } else {
            throw new UnsupportedOperationException("Unknown job type: " + jobType);
        }
    }


}

