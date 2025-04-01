package com.boluo.process;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

/**
 * @author chao
 * @datetime 2025-04-01 21:16
 * @description
 */
@Component
public class JobTradeProcess implements Process {

    @Override
    public Dataset<Row> processBronzeData(Dataset<Row> ds) {
        return null;
    }

}
