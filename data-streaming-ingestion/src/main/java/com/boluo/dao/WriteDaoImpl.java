package com.boluo.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.stereotype.Component;
import utils.SparkUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author chao
 * @datetime 2025-02-03 19:05
 * @description
 */
@Component
public class WriteDaoImpl {

    public void write(Map<String, String> params) throws TimeoutException, StreamingQueryException {

        // org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(Ljava/lang/String;I)Z
        // 要导入 hadoop.dll 文件

        SparkSession spark = SparkUtils.initialSpark();
        Dataset<Row> kafkaDs = spark.readStream().format("kafka")
                .options(params)
                .load();

        kafkaDs = processJsonData(kafkaDs);
        kafkaDs.writeStream()
                .foreachBatch((batchDs, batchId) -> {
                    System.out.println("batchId" + batchId);
                    if (!batchDs.isEmpty()) {
                        batchDs.show(false);
                    }
                }).start().awaitTermination();

    }

    public static Dataset<Row> processJsonData(Dataset<Row> ds) {
        return ds.selectExpr(
                "cast(offset as string) as kafka_offset_num",
                "cast(partition as string) as kafka_partition",
                "cast(topic as string) as kafka_topic",
                "cast(value as string) as raw_msg",
                "cast(null as string) as raw_msg_schema"
        );
    }


}
