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
                        // batchDs.show(false);
                        String jdbcUrl = "jdbc:mysql://mysql.sqlpub.com:3306/dingchao_db?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
                        String tableName = "bronze_trade";
                        String userName = "dingchao";
                        String password = "6049773e1914457d";
                        SparkUtils.writeToMySQL(batchDs, jdbcUrl, tableName, userName, password);
                    }
                }).start().awaitTermination();

        // +----------------+---------------+-----------+----------+--------------+
        // |kafka_offset_num|kafka_partition|kafka_topic|raw_msg   |raw_msg_schema|
        // +----------------+---------------+-----------+----------+--------------+
        // |30              |0              |test       |Message: 0|null          |
        // |31              |0              |test       |Message: 1|null          |
        // |32              |0              |test       |Message: 2|null          |
        // |33              |0              |test       |Message: 3|null          |
        // |34              |0              |test       |Message: 4|null          |
        // +----------------+---------------+-----------+----------+--------------+

    }

    public static Dataset<Row> processJsonData(Dataset<Row> ds) {
        return ds.selectExpr(
                "cast(topic as string) as kafka_topic",
                "cast(partition as bigint) as kafka_partition",
                "cast(offset as bigint) as kafka_offset",
                "cast(value as string) as kafka_raw_msg"
        );
    }


}
