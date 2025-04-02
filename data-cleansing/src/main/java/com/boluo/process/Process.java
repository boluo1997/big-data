package com.boluo.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @author chao
 * @datetime 2025-04-01 21:16
 * @description
 */
public interface Process {

    Dataset<Row> processBronzeData(Dataset<Row> ds);

}
