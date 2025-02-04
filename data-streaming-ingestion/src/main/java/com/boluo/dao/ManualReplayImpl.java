package com.boluo.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.stereotype.Component;
import utils.SparkUtils;

import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * @author chao
 * @datetime 2025-02-03 20:13
 * @description
 */
@Component
public class ManualReplayImpl {

    public void write(Map<String, String> params) throws TimeoutException, StreamingQueryException {

        SparkSession spark = SparkUtils.initialSpark();
        Dataset<Row> csvDs = spark.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("C:\\Users\\dingc\\IdeaProjects\\big-data\\doc\\streaming-ingestion\\1.csv");

        csvDs.show(false);

        // link:
        params.put("startingOffsets", "{\"test\":{\"0\":21}}");
        params.put("endingOffsets", "{\"test\":{\"0\":23}}");

        Dataset<Row> kafkaDs = spark.read().format("kafka")
                .options(params)
                .load();

        kafkaDs = processJsonData(kafkaDs);
        kafkaDs.show(false);

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
